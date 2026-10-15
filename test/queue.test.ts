import { beforeEach, describe, expect, it } from "vitest";
import { Queue, backoffMs } from "../src/queue.js";

let clock: Date;
let q: Queue;
const advance = (ms: number) => { clock = new Date(clock.getTime() + ms); };

beforeEach(() => {
  clock = new Date("2026-10-14T12:00:00Z");
  q = new Queue(":memory:", { now: () => clock });
});

describe("enqueue and claim", () => {
  it("claims by priority, then run_at, then id", () => {
    q.enqueue("t", { n: 1 });
    q.enqueue("t", { n: 2 }, { priority: 5 });
    q.enqueue("t", { n: 3 }, { priority: 5 });
    const order = [q.claim("w", ["t"]), q.claim("w", ["t"]), q.claim("w", ["t"])].map((j) => (j!.payload as { n: number }).n);
    expect(order).toEqual([2, 3, 1]);
    expect(q.claim("w", ["t"])).toBeUndefined();
  });

  it("does not hand out delayed jobs until they are due", () => {
    q.enqueue("t", {}, { delayMs: 60_000 });
    expect(q.claim("w", ["t"])).toBeUndefined();
    advance(59_999);
    expect(q.claim("w", ["t"])).toBeUndefined();
    advance(1);
    expect(q.claim("w", ["t"])?.state).toBe("running");
  });

  it("only claims registered types", () => {
    q.enqueue("a");
    expect(q.claim("w", ["b"])).toBeUndefined();
    expect(q.claim("w", [])).toBeUndefined();
    expect(q.claim("w", ["b", "a"])?.type).toBe("a");
  });

  it("dedupes on idempotency key", () => {
    const first = q.enqueue("t", { v: 1 }, { idempotencyKey: "k" });
    const second = q.enqueue("t", { v: 2 }, { idempotencyKey: "k" });
    expect(first.created).toBe(true);
    expect(second.created).toBe(false);
    expect(second.job.id).toBe(first.job.id);
    expect(q.stats().byState.queued).toBe(1);
  });
});

describe("success, failure, retries", () => {
  it("succeed only works for the worker holding the job", () => {
    const { job } = q.enqueue("t");
    q.claim("w1", ["t"]);
    expect(q.succeed(job.id, "w2")).toBe(false);
    expect(q.succeed(job.id, "w1")).toBe(true);
    expect(q.get(job.id)?.state).toBe("succeeded");
    expect(q.events(job.id).map((e) => e.event)).toEqual(["enqueued", "started", "succeeded"]);
  });

  it("retries with growing backoff and dies after max attempts", () => {
    const { job } = q.enqueue("t", {}, { maxAttempts: 3 });
    const delays: number[] = [];
    for (let attempt = 1; attempt <= 3; attempt++) {
      const claimed = q.claim("w", ["t"]);
      expect(claimed?.attempts).toBe(attempt);
      const outcome = q.fail(job.id, "w", `boom ${attempt}`, { baseMs: 1000 });
      if (attempt < 3) {
        expect(outcome).toBe("retry");
        const j = q.get(job.id)!;
        expect(j.state).toBe("failed");
        delays.push(new Date(j.run_at).getTime() - clock.getTime());
        expect(q.claim("w", ["t"])).toBeUndefined(); // backoff not elapsed
        advance(delays.at(-1)!);
      } else {
        expect(outcome).toBe("dead");
      }
    }
    expect(delays[1]).toBeGreaterThan(delays[0]);
    const dead = q.get(job.id)!;
    expect(dead.state).toBe("dead");
    expect(dead.last_error).toBe("boom 3");
    expect(q.claim("w", ["t"])).toBeUndefined();
  });

  it("backoff doubles per attempt with bounded jitter and a cap", () => {
    const fixed = () => 0.5; // jitter factor 1.0
    expect(backoffMs(1, 1000, 60_000, fixed)).toBe(1000);
    expect(backoffMs(2, 1000, 60_000, fixed)).toBe(2000);
    expect(backoffMs(5, 1000, 60_000, fixed)).toBe(16000);
    expect(backoffMs(20, 1000, 60_000, fixed)).toBe(60_000);
    expect(backoffMs(1, 1000, 60_000, () => 0)).toBe(800);
    expect(backoffMs(1, 1000, 60_000, () => 1)).toBe(1200);
  });

  it("fail is ignored from a worker that does not own the job", () => {
    const { job } = q.enqueue("t");
    q.claim("w1", ["t"]);
    expect(q.fail(job.id, "w2", "x")).toBe("ignored");
    expect(q.get(job.id)?.state).toBe("running");
  });

  it("retryDead requeues with a fresh attempt budget; cancel stops queued or failed jobs", () => {
    const { job } = q.enqueue("t", {}, { maxAttempts: 1 });
    q.claim("w", ["t"]);
    expect(q.fail(job.id, "w", "x")).toBe("dead");
    expect(q.retryDead(job.id)).toBe(true);
    expect(q.get(job.id)).toMatchObject({ state: "queued", attempts: 0, last_error: null });
    expect(q.cancel(job.id)).toBe(true);
    expect(q.cancel(job.id)).toBe(false);
    expect(q.claim("w", ["t"])).toBeUndefined();
  });
});

describe("leases and crash recovery", () => {
  it("reclaims a running job whose lease expired, and records why", () => {
    const { job } = q.enqueue("t");
    expect(q.claim("w1", ["t"], 30_000)?.id).toBe(job.id);
    advance(29_000);
    expect(q.claim("w2", ["t"], 30_000)).toBeUndefined(); // still leased
    expect(q.heartbeat(job.id, "w1", 30_000)).toBe(true); // w1 is alive, extends lease
    advance(29_000);
    expect(q.claim("w2", ["t"], 30_000)).toBeUndefined();
    advance(2_000); // w1 stopped heartbeating -> lease expired
    const reclaimed = q.claim("w2", ["t"], 30_000);
    expect(reclaimed).toMatchObject({ id: job.id, worker_id: "w2", attempts: 2 });
    expect(q.events(job.id).some((e) => e.event === "lease_expired")).toBe(true);
    expect(q.succeed(job.id, "w1")).toBe(false); // the dead worker can no longer complete it
    expect(q.heartbeat(job.id, "w1")).toBe(false);
  });
});

describe("schedules", () => {
  it("enqueues once per due time, with idempotency across ticks", () => {
    q.addSchedule("hourly", "0 * * * *", "report", { kind: "hourly" });
    expect(q.schedules()[0].next_run_at).toBe("2026-10-14T13:00:00.000Z");
    expect(q.tickSchedules()).toBe(0);
    advance(60 * 60_000); // 13:00
    expect(q.tickSchedules()).toBe(1);
    expect(q.tickSchedules()).toBe(0); // next_run_at moved to 14:00
    const jobs = q.list({ type: "report" });
    expect(jobs).toHaveLength(1);
    expect(jobs[0].schedule_id).toBe(q.schedules()[0].id);
    expect(q.schedules()[0].next_run_at).toBe("2026-10-14T14:00:00.000Z");
  });

  it("upserts by name, can be disabled and removed", () => {
    q.addSchedule("s", "0 0 * * *", "a");
    q.addSchedule("s", "30 0 * * *", "b");
    expect(q.schedules()).toHaveLength(1);
    expect(q.schedules()[0]).toMatchObject({ cron: "30 0 * * *", type: "b" });
    expect(q.setScheduleEnabled("s", false)).toBe(true);
    advance(2 * 86400_000);
    expect(q.tickSchedules()).toBe(0);
    expect(q.removeSchedule("s")).toBe(true);
    expect(q.removeSchedule("s")).toBe(false);
    expect(() => q.addSchedule("bad", "nope", "a")).toThrow();
  });
});

describe("stats, list, prune", () => {
  it("counts by state and type, lists newest first, prunes old finished jobs", () => {
    q.enqueue("a"); q.enqueue("a"); q.enqueue("b");
    q.claim("w", ["a"]);
    q.succeed(1, "w");
    expect(q.stats().byState).toMatchObject({ queued: 2, succeeded: 1 });
    expect(q.stats().byType.a).toMatchObject({ queued: 1, succeeded: 1 });
    expect(q.list().map((j) => j.id)).toEqual([3, 2, 1]);
    expect(q.list({ state: "queued", type: "b" }).map((j) => j.id)).toEqual([3]);
    advance(86400_000);
    expect(q.prune(3600_000)).toBe(1);
    expect(q.get(1)).toBeUndefined();
    expect(q.events(1)).toEqual([]);
  });
});
