/** A small, strict cron parser: five fields (minute hour day-of-month month day-of-week), supporting
 *  `*`, numbers, ranges `a-b`, lists `a,b`, and steps `* /n` or `a-b/n`. Day-of-week is 0-6 (Sunday = 0; 7 also means Sunday).
 *  Standard cron semantics: when both day-of-month and day-of-week are restricted, either may match. */

export interface CronSpec {
  minute: Set<number>;
  hour: Set<number>;
  dom: Set<number>;
  month: Set<number>;
  dow: Set<number>;
  domRestricted: boolean;
  dowRestricted: boolean;
}

const RANGES: Record<string, [number, number]> = { minute: [0, 59], hour: [0, 23], dom: [1, 31], month: [1, 12], dow: [0, 7] };

function parseField(name: string, field: string): Set<number> {
  const [lo, hi] = RANGES[name];
  const out = new Set<number>();
  for (const part of field.split(",")) {
    const m = /^(\*|\d+(?:-\d+)?)(?:\/(\d+))?$/.exec(part);
    if (!m) throw new Error(`invalid cron field '${field}' for ${name}`);
    let start = lo, end = hi;
    if (m[1] !== "*") {
      const [a, b] = m[1].split("-").map(Number);
      start = a; end = b ?? a;
      if (b === undefined && m[2]) end = hi; // "5/10" means starting at 5 every 10
    }
    const step = m[2] ? Number(m[2]) : 1;
    if (step < 1 || start < lo || end > hi || start > end) throw new Error(`cron field '${field}' out of range for ${name}`);
    for (let v = start; v <= end; v += step) out.add(name === "dow" && v === 7 ? 0 : v);
  }
  return out;
}

export function parseCron(expr: string): CronSpec {
  const fields = expr.trim().split(/\s+/);
  if (fields.length !== 5) throw new Error(`cron expression needs 5 fields, got ${fields.length}: '${expr}'`);
  const [mi, h, d, mo, w] = fields;
  return {
    minute: parseField("minute", mi), hour: parseField("hour", h), dom: parseField("dom", d),
    month: parseField("month", mo), dow: parseField("dow", w), domRestricted: d !== "*", dowRestricted: w !== "*",
  };
}

/** Next matching time strictly after `from`, in UTC. Scans minute by minute with day skips; bounded to 5 years. */
export function nextRun(spec: CronSpec, from: Date): Date {
  const t = new Date(from.getTime());
  t.setUTCSeconds(0, 0);
  t.setUTCMinutes(t.getUTCMinutes() + 1);
  const limit = from.getTime() + 5 * 366 * 86400_000;
  while (t.getTime() < limit) {
    if (!spec.month.has(t.getUTCMonth() + 1)) { t.setUTCMonth(t.getUTCMonth() + 1, 1); t.setUTCHours(0, 0); continue; }
    const domOk = spec.dom.has(t.getUTCDate());
    const dowOk = spec.dow.has(t.getUTCDay());
    const dayOk = spec.domRestricted && spec.dowRestricted ? domOk || dowOk : domOk && dowOk;
    if (!dayOk) { t.setUTCDate(t.getUTCDate() + 1); t.setUTCHours(0, 0); continue; }
    if (!spec.hour.has(t.getUTCHours())) { t.setUTCHours(t.getUTCHours() + 1, 0); continue; }
    if (!spec.minute.has(t.getUTCMinutes())) { t.setUTCMinutes(t.getUTCMinutes() + 1); continue; }
    return t;
  }
  throw new Error("no matching time within 5 years");
}
