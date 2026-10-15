import request from "supertest";
import { beforeEach, describe, expect, it } from "vitest";
import { createApp } from "../src/api.js";
import { Queue } from "../src/queue.js";

let q: Queue;
let app: ReturnType<typeof createApp>;
beforeEach(() => { q = new Queue(":memory:"); app = createApp(q); });

describe("jobs API", () => {
  it("enqueues, lists, fetches with events, cancels", async () => {
    const r = await request(app).post("/api/jobs").send({ type: "email.send", payload: { to: "a@b.c" }, priority: 3 });
    expect(r.status).toBe(201);
    expect(r.body).toMatchObject({ type: "email.send", state: "queued", priority: 3 });
    const again = await request(app).post("/api/jobs").send({ type: "email.send", idempotencyKey: "k1" });
    expect(again.status).toBe(201);
    expect((await request(app).post("/api/jobs").send({ type: "email.send", idempotencyKey: "k1" })).status).toBe(200);

    expect((await request(app).get("/api/jobs?state=queued")).body).toHaveLength(2);
    const one = await request(app).get(`/api/jobs/${r.body.id}`);
    expect(one.body.events.map((e: { event: string }) => e.event)).toEqual(["enqueued"]);

    expect((await request(app).post(`/api/jobs/${r.body.id}/cancel`)).body.state).toBe("cancelled");
    expect((await request(app).post(`/api/jobs/${r.body.id}/cancel`)).status).toBe(409);
    expect((await request(app).get("/api/jobs/999")).status).toBe(404);
    expect((await request(app).get("/api/jobs/abc")).status).toBe(404);
  });

  it("validates input", async () => {
    const bad = await request(app).post("/api/jobs").send({ type: "", priority: 1000 });
    expect(bad.status).toBe(400);
    expect(Object.keys(bad.body.details)).toEqual(expect.arrayContaining(["type", "priority"]));
    expect((await request(app).get("/api/jobs?state=bogus")).status).toBe(400);
    expect((await request(app).post("/api/jobs").set("Content-Type", "application/json").send("{")).status).toBe(400);
  });

  it("retries dead jobs via the API", async () => {
    const { job } = q.enqueue("t", {}, { maxAttempts: 1 });
    q.claim("w", ["t"]);
    q.fail(job.id, "w", "x");
    expect((await request(app).post(`/api/jobs/${job.id}/retry`)).body.state).toBe("queued");
    expect((await request(app).post(`/api/jobs/${job.id}/retry`)).status).toBe(409);
  });

  it("manages schedules and reports stats", async () => {
    const r = await request(app).put("/api/schedules").send({ name: "nightly", cron: "0 2 * * *", type: "report" });
    expect(r.status).toBe(201);
    expect(r.body.next_run_at).toMatch(/T02:00:00/);
    expect((await request(app).put("/api/schedules").send({ name: "x", cron: "bad", type: "r" })).status).toBe(400);
    expect((await request(app).post("/api/schedules/nightly/disable")).body).toEqual({ ok: true });
    expect((await request(app).get("/api/schedules")).body[0].enabled).toBe(0);
    expect((await request(app).delete("/api/schedules/nightly")).status).toBe(204);
    expect((await request(app).delete("/api/schedules/nightly")).status).toBe(404);
    const stats = await request(app).get("/api/stats");
    expect(stats.body.byState).toMatchObject({ queued: 0 });
    expect((await request(app).get("/")).text).toContain("<title>jobq</title>");
  });
});
