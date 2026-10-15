import express, { type NextFunction, type Request, type Response } from "express";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { z, ZodError } from "zod";
import { parseCron } from "./cron.js";
import type { JobState, Queue } from "./queue.js";

const STATES = ["queued", "running", "succeeded", "failed", "dead", "cancelled"] as const;

const enqueueSchema = z.object({
  type: z.string().trim().min(1).max(80),
  payload: z.unknown().default({}),
  priority: z.number().int().min(-100).max(100).optional(),
  delayMs: z.number().int().min(0).max(30 * 86400_000).optional(),
  maxAttempts: z.number().int().min(1).max(50).optional(),
  idempotencyKey: z.string().trim().min(1).max(200).optional(),
});

const scheduleSchema = z.object({
  name: z.string().trim().min(1).max(80),
  cron: z.string().trim().refine((c) => { try { parseCron(c); return true; } catch { return false; } }, "invalid cron expression"),
  type: z.string().trim().min(1).max(80),
  payload: z.unknown().default({}),
});

function id(req: Request): number {
  const n = Number(req.params.id);
  if (!Number.isInteger(n) || n <= 0) throw Object.assign(new Error("Not found"), { status: 404 });
  return n;
}

export function createApp(queue: Queue) {
  const app = express();
  app.use(express.json({ limit: "256kb" }));
  const dashboard = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "dashboard.html"), "utf-8");

  app.get("/", (_req, res) => res.type("html").send(dashboard));
  app.get("/api/health", (_req, res) => res.json({ ok: true }));
  app.get("/api/stats", (_req, res) => res.json(queue.stats()));

  app.get("/api/jobs", (req, res) => {
    const q = z.object({ state: z.enum(STATES).optional(), type: z.string().optional(), limit: z.coerce.number().int().min(1).max(500).default(50) }).parse(req.query);
    res.json(queue.list({ state: q.state as JobState | undefined, type: q.type, limit: q.limit }));
  });
  app.post("/api/jobs", (req, res) => {
    const b = enqueueSchema.parse(req.body);
    const { job, created } = queue.enqueue(b.type, b.payload, b);
    res.status(created ? 201 : 200).json(job);
  });
  app.get("/api/jobs/:id", (req, res) => {
    const job = queue.get(id(req));
    if (!job) return res.status(404).json({ error: "Not found" });
    res.json({ ...job, events: queue.events(job.id) });
  });
  app.post("/api/jobs/:id/cancel", (req, res) => {
    if (!queue.cancel(id(req))) return res.status(409).json({ error: "Job is not queued or failed" });
    res.json(queue.get(id(req)));
  });
  app.post("/api/jobs/:id/retry", (req, res) => {
    if (!queue.retryDead(id(req))) return res.status(409).json({ error: "Job is not dead" });
    res.json(queue.get(id(req)));
  });

  app.get("/api/schedules", (_req, res) => res.json(queue.schedules()));
  app.put("/api/schedules", (req, res) => {
    const b = scheduleSchema.parse(req.body);
    res.status(201).json(queue.addSchedule(b.name, b.cron, b.type, b.payload));
  });
  app.post("/api/schedules/:name/enable", (req, res) => res.json({ ok: queue.setScheduleEnabled(String(req.params.name), true) }));
  app.post("/api/schedules/:name/disable", (req, res) => res.json({ ok: queue.setScheduleEnabled(String(req.params.name), false) }));
  app.delete("/api/schedules/:name", (req, res) => (queue.removeSchedule(String(req.params.name)) ? res.status(204).end() : res.status(404).json({ error: "Not found" })));

  app.use("/api", (_req, res) => res.status(404).json({ error: "Not found" }));
  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof ZodError) return res.status(400).json({ error: "Validation failed", details: err.flatten().fieldErrors });
    if (err instanceof SyntaxError && "body" in err) return res.status(400).json({ error: "Malformed JSON body" });
    const status = (err as { status?: number }).status ?? 500;
    if (status === 500) console.error(err);
    return res.status(status).json({ error: status === 500 ? "Internal server error" : (err as Error).message });
  });
  return app;
}
