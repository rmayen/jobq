# jobq

A persistent background job queue on SQLite, with the parts that make a queue safe to rely on: priorities,
delayed jobs, retries with exponential backoff and jitter, a dead-letter state, cron schedules, lease-based
crash recovery, per-job timeouts, graceful shutdown, an audit trail per job, an HTTP API, and a dashboard.

Node 22+, TypeScript, and the built-in `node:sqlite` driver. No queue server to run, no native modules.

## Why it is built this way

- **Claiming is one atomic statement inside `BEGIN IMMEDIATE`.** Two workers polling the same database can
  never take the same job, and the ordering (priority, then due time, then id) is enforced by the query, not
  by application code.
- **Leases, not locks.** A claimed job carries a `lease_until`. The worker heartbeats to extend it while the
  handler runs. If a worker crashes, its lease expires and another worker reclaims the job, and the job's
  event log records that it was reclaimed and from whom. Nothing is lost and nothing needs manual cleanup.
- **Failures are data.** A failed attempt records the error and a `run_at` in the future computed by
  `base * 2^(attempt-1)` with 20% jitter and a cap. After `max_attempts` the job moves to `dead`, where it
  stays visible until someone retries or deletes it. Dead jobs are never silently dropped.
- **Schedules enqueue exactly once.** Each firing uses an idempotency key of `schedule:<name>:<due time>`,
  so overlapping workers or restarts cannot double-enqueue a cron job.
- **Ownership is checked on every write.** `succeed`, `fail`, and `heartbeat` all require the worker id that
  holds the lease, so a worker that was declared dead cannot come back and overwrite a reclaimed job's result.

## Run it

```bash
npm install
npm run demo     # in-memory: enqueue a mix of jobs and a schedule, run a worker for 4s, print the event log
npm run dev      # API + dashboard on http://localhost:3001 with an in-process worker and demo handlers
npm test         # 29 tests
```

Demo output looks like this (note the VIP job finishing first, the delayed report, the retry then dead letter,
and the duplicate welcome email that was never enqueued):

```
[started  ] #2 email.send attempt 1          <- priority 10
[succeeded] #2 email.send attempt 1
[failed   ] #5 always.fails attempt 1 — invalid payload (retry in 212ms)
[dead     ] #5 always.fails attempt 2 — invalid payload
[started  ] #3 report.generate attempt 1     <- delayMs: 1500
stats: {"succeeded":5,"dead":1,...}
schedules: nightly-report next 2026-10-15T02:00:00.000Z
```

## Use it as a library

```ts
import { Queue } from "./src/queue.js";
import { Worker } from "./src/worker.js";

const queue = new Queue("jobq.db");
queue.enqueue("email.send", { to: "ana@example.com" }, { priority: 5, maxAttempts: 5 });
queue.enqueue("report.generate", { month: "2026-09" }, { delayMs: 60_000 });
queue.addSchedule("nightly-report", "0 2 * * *", "report.generate", { month: "auto" });

const worker = new Worker(queue, { concurrency: 4, jobTimeoutMs: 30_000 })
  .register("email.send", async (payload) => { /* ... */ })
  .register("report.generate", async (payload, { signal }) => { /* check signal.aborted in long loops */ });
worker.start();
process.on("SIGTERM", () => worker.stop().then(() => queue.close()));
```

`Worker.drain()` processes everything currently runnable and returns, which is what the tests use and what
a one-shot cron container would call.

## HTTP API

| Method | Path                               | Purpose                                                   |
| ------ | ---------------------------------- | --------------------------------------------------------- |
| POST   | `/api/jobs`                        | `type`, `payload`, `priority`, `delayMs`, `maxAttempts`, `idempotencyKey` (201 new, 200 duplicate) |
| GET    | `/api/jobs?state=&type=&limit=`    | newest first                                              |
| GET    | `/api/jobs/:id`                    | job plus its event log                                    |
| POST   | `/api/jobs/:id/cancel`             | queued or failed jobs only                                |
| POST   | `/api/jobs/:id/retry`              | dead jobs only; resets the attempt budget                 |
| GET    | `/api/stats`                       | counts by state and by type                               |
| PUT    | `/api/schedules`                   | upsert `name`, `cron`, `type`, `payload`                  |
| POST   | `/api/schedules/:name/enable`      | and `/disable`                                            |
| DELETE | `/api/schedules/:name`             |                                                           |

The dashboard at `/` shows live counts, the job table with cancel and retry buttons, and the schedules.

## Cron

Five fields, UTC: minute, hour, day of month, month, day of week. Supports `*`, numbers, `a-b`, `a,b`, and
`*/n` or `a-b/n`. When both day fields are restricted, either may match, like Vixie cron. The parser rejects
anything out of range or malformed.

## Tests

29 tests across four files:

- `cron.test.ts`: field parsing, rejection of malformed input, next-run across minute, hour, day, month, and
  weekday boundaries, including months without a 31st and the day-of-month OR day-of-week rule.
- `queue.test.ts`: ordering, delays, type filtering, idempotency, ownership checks on success and failure,
  backoff growth and jitter bounds, dead-lettering and retry, **lease expiry and reclaim by a second worker**,
  schedule ticks firing exactly once, stats, listing, pruning. Uses an injected clock, so no sleeps.
- `worker.test.ts`: concurrency limit, retry then dead through the worker, handler timeout with abort signal,
  missing handler, heartbeat keeping a long job alive, graceful stop waiting for in-flight work.
- `api.test.ts`: every route, validation, idempotent enqueue status codes, cancel and retry rules, schedules.

## Layout

```
src/
  cron.ts        parser + next-run
  queue.ts       schema, enqueue/claim/succeed/fail, leases, schedules, stats, prune
  worker.ts      handlers, concurrency, heartbeats, timeouts, graceful stop
  api.ts         Express routes; dashboard.html
  server.ts      API + dashboard + in-process worker with demo handlers, SIGTERM drain
  demo.ts        standalone in-memory demonstration
test/            cron, queue, worker, api
```

## Limitations

- One SQLite file means one machine. Workers on several machines would need the database on shared storage or
  a different store; the claim and lease logic would be unchanged.
- Schedules are UTC only.
- Payloads are JSON; large blobs belong elsewhere with a reference in the payload.
