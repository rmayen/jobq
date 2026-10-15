import { randomBytes } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { nextRun, parseCron } from "./cron.js";

export type JobState = "queued" | "running" | "succeeded" | "failed" | "dead" | "cancelled";

export interface Job {
  id: number;
  type: string;
  payload: unknown;
  state: JobState;
  priority: number;
  attempts: number;
  max_attempts: number;
  run_at: string;
  lease_until: string | null;
  worker_id: string | null;
  last_error: string | null;
  idempotency_key: string | null;
  schedule_id: number | null;
  created_at: string;
  updated_at: string;
}

export interface Schedule {
  id: number;
  name: string;
  cron: string;
  type: string;
  payload: unknown;
  enabled: 0 | 1;
  next_run_at: string;
  last_enqueued_at: string | null;
}

export interface EnqueueOptions {
  priority?: number; // higher runs first
  delayMs?: number;
  runAt?: Date;
  maxAttempts?: number;
  idempotencyKey?: string;
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS jobs (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  type            TEXT NOT NULL,
  payload         TEXT NOT NULL,
  state           TEXT NOT NULL DEFAULT 'queued' CHECK (state IN ('queued','running','succeeded','failed','dead','cancelled')),
  priority        INTEGER NOT NULL DEFAULT 0,
  attempts        INTEGER NOT NULL DEFAULT 0,
  max_attempts    INTEGER NOT NULL DEFAULT 3,
  run_at          TEXT NOT NULL,
  lease_until     TEXT,
  worker_id       TEXT,
  last_error      TEXT,
  idempotency_key TEXT UNIQUE,
  schedule_id     INTEGER,
  created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(state, run_at, priority DESC, id);
CREATE INDEX IF NOT EXISTS idx_jobs_type  ON jobs(type, state);
CREATE TABLE IF NOT EXISTS schedules (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  name             TEXT NOT NULL UNIQUE,
  cron             TEXT NOT NULL,
  type             TEXT NOT NULL,
  payload          TEXT NOT NULL,
  enabled          INTEGER NOT NULL DEFAULT 1,
  next_run_at      TEXT NOT NULL,
  last_enqueued_at TEXT
);
CREATE TABLE IF NOT EXISTS job_events (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id   INTEGER NOT NULL,
  at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  event    TEXT NOT NULL,
  detail   TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_job ON job_events(job_id, id);
`;

const iso = (d: Date) => d.toISOString();
const rows = <T>(v: unknown) => v as T[];
const row = <T>(v: unknown) => v as T | undefined;

/** Backoff: base * 2^(attempt-1) with up to 20% jitter, capped. Attempt 1 -> ~1s, 2 -> ~2s, 3 -> ~4s ... */
export function backoffMs(attempt: number, baseMs = 1000, capMs = 10 * 60_000, rand: () => number = Math.random): number {
  const raw = Math.min(capMs, baseMs * 2 ** Math.max(0, attempt - 1));
  return Math.round(raw * (0.8 + 0.4 * rand()));
}

export class Queue {
  readonly db: DatabaseSync;
  private readonly now: () => Date;

  constructor(path: string = process.env.JOBQ_DB ?? "jobq.db", opts: { now?: () => Date } = {}) {
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA journal_mode = WAL;");
    this.db.exec("PRAGMA busy_timeout = 5000;");
    this.db.exec(SCHEMA);
    this.now = opts.now ?? (() => new Date());
  }

  private parse(r: Record<string, unknown>): Job {
    return { ...(r as unknown as Job), payload: JSON.parse(r.payload as string) };
  }

  private event(jobId: number, event: string, detail?: string): void {
    this.db.prepare("INSERT INTO job_events (job_id, event, detail) VALUES (?, ?, ?)").run(jobId, event, detail ?? null);
  }

  // ---- producing
  enqueue(type: string, payload: unknown = {}, opts: EnqueueOptions = {}): { job: Job; created: boolean } {
    const runAt = opts.runAt ?? new Date(this.now().getTime() + (opts.delayMs ?? 0));
    if (opts.idempotencyKey) {
      const existing = row<Record<string, unknown>>(this.db.prepare("SELECT * FROM jobs WHERE idempotency_key = ?").get(opts.idempotencyKey));
      if (existing) return { job: this.parse(existing), created: false };
    }
    const res = this.db
      .prepare("INSERT INTO jobs (type, payload, priority, max_attempts, run_at, idempotency_key) VALUES (?, ?, ?, ?, ?, ?)")
      .run(type, JSON.stringify(payload), opts.priority ?? 0, opts.maxAttempts ?? 3, iso(runAt), opts.idempotencyKey ?? null);
    const id = Number(res.lastInsertRowid);
    this.event(id, "enqueued");
    return { job: this.get(id)!, created: true };
  }

  get(id: number): Job | undefined {
    const r = row<Record<string, unknown>>(this.db.prepare("SELECT * FROM jobs WHERE id = ?").get(id));
    return r && this.parse(r);
  }

  cancel(id: number): boolean {
    const res = this.db.prepare("UPDATE jobs SET state = 'cancelled', updated_at = ? WHERE id = ? AND state IN ('queued','failed')").run(iso(this.now()), id);
    if (res.changes) this.event(id, "cancelled");
    return res.changes > 0;
  }

  /** Put a dead job back in the queue with a fresh attempt budget. */
  retryDead(id: number): boolean {
    const res = this.db.prepare("UPDATE jobs SET state = 'queued', attempts = 0, run_at = ?, last_error = NULL, updated_at = ? WHERE id = ? AND state = 'dead'").run(iso(this.now()), iso(this.now()), id);
    if (res.changes) this.event(id, "requeued_from_dead");
    return res.changes > 0;
  }

  // ---- consuming
  /** Atomically claim the next runnable job for `types`. The lease must be renewed or completed before it expires,
   *  or another worker may reclaim the job. Expired leases are what make crash recovery work. */
  claim(workerId: string, types: string[], leaseMs = 30_000): Job | undefined {
    if (types.length === 0) return undefined;
    const now = this.now();
    const until = iso(new Date(now.getTime() + leaseMs));
    const marks = types.map(() => "?").join(",");
    this.db.exec("BEGIN IMMEDIATE");
    try {
      // 'failed' means waiting for its backoff; 'running' with an expired lease means its worker died.
      const r = row<Record<string, unknown>>(this.db.prepare(
        `SELECT * FROM jobs
         WHERE type IN (${marks})
           AND ((state IN ('queued','failed') AND run_at <= ?) OR (state = 'running' AND lease_until < ?))
         ORDER BY priority DESC, run_at ASC, id ASC LIMIT 1`,
      ).get(...types, iso(now), iso(now)));
      if (!r) { this.db.exec("COMMIT"); return undefined; }
      const id = r.id as number;
      if (r.state === "running") this.event(id, "lease_expired", `reclaimed from ${r.worker_id}`);
      this.db.prepare("UPDATE jobs SET state = 'running', attempts = attempts + 1, lease_until = ?, worker_id = ?, updated_at = ? WHERE id = ?")
        .run(until, workerId, iso(now), id);
      this.event(id, "started", workerId);
      this.db.exec("COMMIT");
      return this.get(id);
    } catch (e) {
      this.db.exec("ROLLBACK");
      throw e;
    }
  }

  heartbeat(id: number, workerId: string, leaseMs = 30_000): boolean {
    const until = iso(new Date(this.now().getTime() + leaseMs));
    return this.db.prepare("UPDATE jobs SET lease_until = ? WHERE id = ? AND worker_id = ? AND state = 'running'").run(until, id, workerId).changes > 0;
  }

  succeed(id: number, workerId: string): boolean {
    const res = this.db.prepare("UPDATE jobs SET state = 'succeeded', lease_until = NULL, updated_at = ? WHERE id = ? AND worker_id = ? AND state = 'running'").run(iso(this.now()), id, workerId);
    if (res.changes) this.event(id, "succeeded");
    return res.changes > 0;
  }

  /** Record a failure: schedule a retry with backoff, or move to the dead-letter state when attempts are exhausted. */
  fail(id: number, workerId: string, error: string, opts: { baseMs?: number } = {}): "retry" | "dead" | "ignored" {
    const job = this.get(id);
    if (!job || job.worker_id !== workerId || job.state !== "running") return "ignored";
    const now = this.now();
    if (job.attempts >= job.max_attempts) {
      this.db.prepare("UPDATE jobs SET state = 'dead', lease_until = NULL, last_error = ?, updated_at = ? WHERE id = ?").run(error, iso(now), id);
      this.event(id, "dead", error);
      return "dead";
    }
    const delay = backoffMs(job.attempts, opts.baseMs);
    this.db.prepare("UPDATE jobs SET state = 'failed', lease_until = NULL, last_error = ?, run_at = ?, updated_at = ? WHERE id = ?")
      .run(error, iso(new Date(now.getTime() + delay)), iso(now), id);
    this.event(id, "failed", `${error} (retry in ${delay}ms)`);
    return "retry";
  }

  // ---- schedules
  addSchedule(name: string, cron: string, type: string, payload: unknown = {}): Schedule {
    const spec = parseCron(cron);
    const next = iso(nextRun(spec, this.now()));
    this.db.prepare(
      `INSERT INTO schedules (name, cron, type, payload, next_run_at) VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(name) DO UPDATE SET cron = excluded.cron, type = excluded.type, payload = excluded.payload, next_run_at = excluded.next_run_at, enabled = 1`,
    ).run(name, cron, type, JSON.stringify(payload), next);
    return this.schedules().find((s) => s.name === name)!;
  }

  setScheduleEnabled(name: string, enabled: boolean): boolean {
    return this.db.prepare("UPDATE schedules SET enabled = ? WHERE name = ?").run(enabled ? 1 : 0, name).changes > 0;
  }

  removeSchedule(name: string): boolean {
    return this.db.prepare("DELETE FROM schedules WHERE name = ?").run(name).changes > 0;
  }

  schedules(): Schedule[] {
    return rows<Record<string, unknown>>(this.db.prepare("SELECT * FROM schedules ORDER BY name").all())
      .map((r) => ({ ...(r as unknown as Schedule), payload: JSON.parse(r.payload as string) }));
  }

  /** Enqueue a job for every due schedule, exactly once per tick, using an idempotency key of name + due time. */
  tickSchedules(): number {
    const now = this.now();
    let n = 0;
    for (const s of this.schedules()) {
      if (!s.enabled || s.next_run_at > iso(now)) continue;
      const { created } = this.enqueue(s.type, s.payload, { idempotencyKey: `schedule:${s.name}:${s.next_run_at}` });
      const next = iso(nextRun(parseCron(s.cron), now));
      this.db.prepare("UPDATE schedules SET next_run_at = ?, last_enqueued_at = ? WHERE id = ?").run(next, iso(now), s.id);
      if (created) {
        this.db.prepare("UPDATE jobs SET schedule_id = ? WHERE idempotency_key = ?").run(s.id, `schedule:${s.name}:${s.next_run_at}`);
        n++;
      }
    }
    return n;
  }

  // ---- observing
  list(opts: { state?: JobState; type?: string; limit?: number } = {}): Job[] {
    const where: string[] = [];
    const params: (string | number)[] = [];
    if (opts.state) { where.push("state = ?"); params.push(opts.state); }
    if (opts.type) { where.push("type = ?"); params.push(opts.type); }
    const clause = where.length ? `WHERE ${where.join(" AND ")}` : "";
    return rows<Record<string, unknown>>(this.db.prepare(`SELECT * FROM jobs ${clause} ORDER BY id DESC LIMIT ?`).all(...params, opts.limit ?? 50)).map((r) => this.parse(r));
  }

  events(jobId: number): { at: string; event: string; detail: string | null }[] {
    return rows<{ at: string; event: string; detail: string | null }>(this.db.prepare("SELECT at, event, detail FROM job_events WHERE job_id = ? ORDER BY id").all(jobId));
  }

  stats(): { byState: Record<JobState, number>; byType: Record<string, Record<JobState, number>>; schedules: number } {
    const states: JobState[] = ["queued", "running", "succeeded", "failed", "dead", "cancelled"];
    const empty = () => Object.fromEntries(states.map((s) => [s, 0])) as Record<JobState, number>;
    const byState = empty();
    const byType: Record<string, Record<JobState, number>> = {};
    for (const r of rows<{ type: string; state: JobState; n: number }>(this.db.prepare("SELECT type, state, COUNT(*) AS n FROM jobs GROUP BY type, state").all())) {
      byState[r.state] += r.n;
      (byType[r.type] ??= empty())[r.state] = r.n;
    }
    const schedules = row<{ n: number }>(this.db.prepare("SELECT COUNT(*) AS n FROM schedules").get())!.n;
    return { byState, byType, schedules };
  }

  /** Delete finished jobs older than `olderThanMs`. Returns rows removed. */
  prune(olderThanMs: number): number {
    const cutoff = iso(new Date(this.now().getTime() - olderThanMs));
    const ids = rows<{ id: number }>(this.db.prepare("SELECT id FROM jobs WHERE state IN ('succeeded','cancelled') AND updated_at < ?").all(cutoff)).map((r) => r.id);
    if (!ids.length) return 0;
    const marks = ids.map(() => "?").join(",");
    this.db.prepare(`DELETE FROM job_events WHERE job_id IN (${marks})`).run(...ids);
    return Number(this.db.prepare(`DELETE FROM jobs WHERE id IN (${marks})`).run(...ids).changes);
  }

  close(): void {
    this.db.close();
  }
}

export const newWorkerId = (prefix = "worker") => `${prefix}-${process.pid}-${randomBytes(3).toString("hex")}`;
