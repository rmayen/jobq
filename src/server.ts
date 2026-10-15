import { createServer } from "node:http";
import { createApp } from "./api.js";
import { Queue } from "./queue.js";
import { Worker } from "./worker.js";

/** API + dashboard + an in-process worker with a few demo handlers. In production you would run workers separately. */
const queue = new Queue();
const app = createApp(queue);
const port = Number(process.env.PORT ?? 3001);

const worker = new Worker(queue, {
  concurrency: Number(process.env.JOBQ_CONCURRENCY ?? 4),
  onEvent: (e) => console.log(`[${e.kind}] #${e.job.id} ${e.job.type}${e.error ? ` — ${e.error}` : ""}`),
})
  .register("email.send", async (p) => { await new Promise((r) => setTimeout(r, 300)); return p; })
  .register("report.generate", async (p, { signal }) => {
    for (let i = 0; i < 10; i++) { if (signal.aborted) throw signal.reason; await new Promise((r) => setTimeout(r, 100)); }
    return p;
  })
  .register("flaky.task", async () => { if (Math.random() < 0.6) throw new Error("upstream returned 503"); })
  .register("always.fails", async () => { throw new Error("invalid payload"); });

worker.start();
const server = createServer(app).listen(port, () => console.log(`jobq API + dashboard on http://localhost:${port} (worker ${worker.id})`));

for (const sig of ["SIGINT", "SIGTERM"] as const) {
  process.on(sig, async () => {
    console.log(`\n${sig}: draining in-flight jobs…`);
    server.close();
    await worker.stop();
    queue.close();
    process.exit(0);
  });
}
