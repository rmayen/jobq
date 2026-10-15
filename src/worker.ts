import { setTimeout as sleep } from "node:timers/promises";
import { newWorkerId, type Job, type Queue } from "./queue.js";

export type Handler = (payload: unknown, ctx: { job: Job; signal: AbortSignal }) => Promise<unknown> | unknown;

export interface WorkerOptions {
  concurrency?: number;
  pollMs?: number;
  leaseMs?: number;
  heartbeatMs?: number;
  jobTimeoutMs?: number;
  backoffBaseMs?: number;
  id?: string;
  onEvent?: (e: { kind: "started" | "succeeded" | "failed" | "dead" | "timeout"; job: Job; error?: string }) => void;
}

/** Pulls jobs for its registered handlers, runs up to `concurrency` at once, renews leases while a job runs,
 *  enforces a per-job timeout, and reports success or failure back to the queue. Stops cleanly on `stop()`. */
export class Worker {
  readonly id: string;
  private handlers = new Map<string, Handler>();
  private running = new Set<Promise<void>>();
  private stopping = false;
  private loop: Promise<void> | null = null;
  private readonly o: Required<Omit<WorkerOptions, "id" | "onEvent">> & Pick<WorkerOptions, "onEvent">;

  constructor(private queue: Queue, opts: WorkerOptions = {}) {
    this.id = opts.id ?? newWorkerId();
    this.o = {
      concurrency: opts.concurrency ?? 2, pollMs: opts.pollMs ?? 250, leaseMs: opts.leaseMs ?? 30_000,
      heartbeatMs: opts.heartbeatMs ?? 10_000, jobTimeoutMs: opts.jobTimeoutMs ?? 5 * 60_000,
      backoffBaseMs: opts.backoffBaseMs ?? 1000, onEvent: opts.onEvent,
    };
  }

  register(type: string, handler: Handler): this {
    this.handlers.set(type, handler);
    return this;
  }

  get types(): string[] {
    return [...this.handlers.keys()];
  }

  start(): void {
    if (this.loop) return;
    this.stopping = false;
    this.loop = this.run();
  }

  /** Stop polling and wait for in-flight jobs to finish (they keep their leases alive meanwhile). */
  async stop(): Promise<void> {
    this.stopping = true;
    await this.loop;
    await Promise.allSettled(this.running);
    this.loop = null;
  }

  /** Process everything currently runnable, then return. Handy for tests and cron-style one-shot runs. */
  async drain(): Promise<number> {
    let n = 0;
    for (;;) {
      const job = this.queue.claim(this.id, this.types, this.o.leaseMs);
      if (!job) break;
      await this.execute(job);
      n++;
    }
    return n;
  }

  private async run(): Promise<void> {
    while (!this.stopping) {
      let claimed = false;
      while (this.running.size < this.o.concurrency && !this.stopping) {
        this.queue.tickSchedules();
        const job = this.queue.claim(this.id, this.types, this.o.leaseMs);
        if (!job) break;
        claimed = true;
        const p = this.execute(job).finally(() => this.running.delete(p));
        this.running.add(p);
      }
      if (!claimed) await sleep(this.o.pollMs);
    }
  }

  private async execute(job: Job): Promise<void> {
    const handler = this.handlers.get(job.type);
    this.o.onEvent?.({ kind: "started", job });
    if (!handler) {
      this.report(job, `no handler registered for '${job.type}'`);
      return;
    }
    const ac = new AbortController();
    const beat = setInterval(() => this.queue.heartbeat(job.id, this.id, this.o.leaseMs), this.o.heartbeatMs);
    const timer = setTimeout(() => ac.abort(new Error(`timed out after ${this.o.jobTimeoutMs}ms`)), this.o.jobTimeoutMs);
    try {
      await Promise.race([
        Promise.resolve(handler(job.payload, { job, signal: ac.signal })),
        new Promise((_, rej) => ac.signal.addEventListener("abort", () => rej(ac.signal.reason), { once: true })),
      ]);
      this.queue.succeed(job.id, this.id);
      this.o.onEvent?.({ kind: "succeeded", job });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (ac.signal.aborted) this.o.onEvent?.({ kind: "timeout", job, error: msg });
      this.report(job, msg);
    } finally {
      clearInterval(beat);
      clearTimeout(timer);
    }
  }

  private report(job: Job, error: string): void {
    const outcome = this.queue.fail(job.id, this.id, error, { baseMs: this.o.backoffBaseMs });
    this.o.onEvent?.({ kind: outcome === "dead" ? "dead" : "failed", job, error });
  }
}
