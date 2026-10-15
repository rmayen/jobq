import { describe, expect, it } from "vitest";
import { Queue } from "../src/queue.js";
import { Worker } from "../src/worker.js";

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("Worker", () => {
  it("runs handlers, respects concurrency, and reports outcomes", async () => {
    const q = new Queue(":memory:");
    let active = 0, peak = 0;
    const seen: string[] = [];
    const w = new Worker(q, { concurrency: 2, pollMs: 10, onEvent: (e) => seen.push(`${e.kind}:${e.job.id}`) })
      .register("slow", async () => { active++; peak = Math.max(peak, active); await wait(60); active--; });
    for (let i = 0; i < 5; i++) q.enqueue("slow");
    w.start();
    await wait(400);
    await w.stop();
    expect(peak).toBe(2);
    expect(q.stats().byState.succeeded).toBe(5);
    expect(seen.filter((s) => s.startsWith("succeeded"))).toHaveLength(5);
  });

  it("marks a job failed then dead through the normal retry path", async () => {
    const q = new Queue(":memory:");
    const w = new Worker(q, { backoffBaseMs: 10 }).register("bad", () => { throw new Error("nope"); });
    const { job } = q.enqueue("bad", {}, { maxAttempts: 2 });
    expect(await w.drain()).toBe(1);
    expect(q.get(job.id)?.state).toBe("failed");
    await wait(40);
    expect(await w.drain()).toBe(1);
    expect(q.get(job.id)).toMatchObject({ state: "dead", attempts: 2, last_error: "nope" });
  });

  it("times out a hung handler and passes an abort signal", async () => {
    const q = new Queue(":memory:");
    let aborted = false;
    const w = new Worker(q, { jobTimeoutMs: 50, backoffBaseMs: 10 })
      .register("hang", (_p, { signal }) => new Promise((_, rej) => signal.addEventListener("abort", () => { aborted = true; rej(signal.reason); })));
    const { job } = q.enqueue("hang", {}, { maxAttempts: 1 });
    await w.drain();
    expect(aborted).toBe(true);
    expect(q.get(job.id)).toMatchObject({ state: "dead" });
    expect(q.get(job.id)?.last_error).toMatch(/timed out after 50ms/);
  });

  it("fails jobs with no registered handler instead of dropping them", async () => {
    const q = new Queue(":memory:");
    const w = new Worker(q, { backoffBaseMs: 10 }).register("known", () => {});
    const { job } = q.enqueue("known", {}, { maxAttempts: 1 });
    // Simulate a job type this worker claims but whose handler was removed between claim and execute.
    (w as unknown as { handlers: Map<string, unknown> }).handlers.delete("known");
    (w as unknown as { handlers: Map<string, unknown> }).handlers.set("known", undefined);
    await w.drain();
    expect(q.get(job.id)?.last_error).toMatch(/no handler/);
  });

  it("keeps the lease alive with heartbeats during a long job", async () => {
    const q = new Queue(":memory:");
    const w = new Worker(q, { leaseMs: 60, heartbeatMs: 20 }).register("long", async () => { await wait(150); });
    const { job } = q.enqueue("long");
    const p = w.drain();
    await wait(100);
    const mid = q.get(job.id)!;
    expect(mid.state).toBe("running");
    expect(new Date(mid.lease_until!).getTime()).toBeGreaterThan(Date.now());
    await p;
    expect(q.get(job.id)?.state).toBe("succeeded");
  });

  it("stop() waits for in-flight work before returning", async () => {
    const q = new Queue(":memory:");
    let finished = false;
    const w = new Worker(q, { pollMs: 5 }).register("x", async () => { await wait(80); finished = true; });
    q.enqueue("x");
    w.start();
    await wait(20);
    await w.stop();
    expect(finished).toBe(true);
    expect(q.stats().byState.succeeded).toBe(1);
  });
});
