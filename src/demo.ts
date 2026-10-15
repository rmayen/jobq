import { Queue } from "./queue.js";
import { Worker } from "./worker.js";

/** Standalone demo: enqueue a mix of jobs and a schedule, run a worker for a few seconds, print what happened. */
const queue = new Queue(":memory:");
queue.enqueue("email.send", { to: "ana@example.com" });
queue.enqueue("email.send", { to: "vip@example.com" }, { priority: 10 });
queue.enqueue("report.generate", { month: "2026-09" }, { delayMs: 1500 });
queue.enqueue("flaky.task", {}, { maxAttempts: 4 });
queue.enqueue("always.fails", {}, { maxAttempts: 2 });
queue.enqueue("email.send", { to: "dup@example.com" }, { idempotencyKey: "welcome:42" });
queue.enqueue("email.send", { to: "dup@example.com" }, { idempotencyKey: "welcome:42" }); // ignored
queue.addSchedule("nightly-report", "0 2 * * *", "report.generate", { month: "auto" });

const worker = new Worker(queue, {
  concurrency: 2, backoffBaseMs: 200, pollMs: 50,
  onEvent: (e) => console.log(`${new Date().toISOString().slice(11, 23)} [${e.kind.padEnd(9)}] #${e.job.id} ${e.job.type} attempt ${e.job.attempts}${e.error ? ` — ${e.error}` : ""}`),
})
  .register("email.send", async (p) => { await new Promise((r) => setTimeout(r, 100)); return p; })
  .register("report.generate", async () => { await new Promise((r) => setTimeout(r, 200)); })
  .register("flaky.task", async () => { if (Math.random() < 0.6) throw new Error("upstream 503"); })
  .register("always.fails", async () => { throw new Error("invalid payload"); });

worker.start();
await new Promise((r) => setTimeout(r, 4000));
await worker.stop();
console.log("\nstats:", JSON.stringify(queue.stats().byState));
console.log("schedules:", queue.schedules().map((s) => `${s.name} next ${s.next_run_at}`).join(", "));
queue.close();
