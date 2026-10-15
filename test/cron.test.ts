import { describe, expect, it } from "vitest";
import { nextRun, parseCron } from "../src/cron.js";

const at = (s: string) => new Date(s);
const next = (expr: string, from: string) => nextRun(parseCron(expr), at(from)).toISOString();

describe("parseCron", () => {
  it("parses stars, lists, ranges and steps", () => {
    const s = parseCron("*/15 9-17 1,15 * 1-5");
    expect([...s.minute]).toEqual([0, 15, 30, 45]);
    expect([...s.hour]).toEqual([9, 10, 11, 12, 13, 14, 15, 16, 17]);
    expect([...s.dom]).toEqual([1, 15]);
    expect(s.month.size).toBe(12);
    expect([...s.dow]).toEqual([1, 2, 3, 4, 5]);
  });
  it("treats 7 as Sunday", () => {
    expect([...parseCron("0 0 * * 7").dow]).toEqual([0]);
  });
  it("rejects malformed expressions", () => {
    for (const bad of ["* * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * * 13 *", "a * * * *", "*/0 * * * *", "10-5 * * * *"]) {
      expect(() => parseCron(bad), bad).toThrow();
    }
  });
});

describe("nextRun", () => {
  it("finds the next minute, hour, and day boundaries", () => {
    expect(next("* * * * *", "2026-10-14T01:30:20Z")).toBe("2026-10-14T01:31:00.000Z");
    expect(next("0 * * * *", "2026-10-14T01:30:00Z")).toBe("2026-10-14T02:00:00.000Z");
    expect(next("0 2 * * *", "2026-10-14T03:00:00Z")).toBe("2026-10-15T02:00:00.000Z");
    expect(next("30 2 * * *", "2026-10-14T02:30:00Z")).toBe("2026-10-15T02:30:00.000Z"); // strictly after
  });
  it("honors day-of-week and month", () => {
    // 2026-10-14 is a Wednesday
    expect(next("0 9 * * 1", "2026-10-14T00:00:00Z")).toBe("2026-10-19T09:00:00.000Z");
    expect(next("0 0 1 1 *", "2026-10-14T00:00:00Z")).toBe("2027-01-01T00:00:00.000Z");
    expect(next("0 12 31 * *", "2026-10-31T13:00:00Z")).toBe("2026-12-31T12:00:00.000Z"); // November has no 31st
  });
  it("uses OR when both day fields are restricted, like real cron", () => {
    // 15th OR a Monday, whichever comes first after Wed Oct 14
    expect(next("0 0 15 * 1", "2026-10-14T00:00:00Z")).toBe("2026-10-15T00:00:00.000Z");
    expect(next("0 0 25 * 1", "2026-10-14T00:00:00Z")).toBe("2026-10-19T00:00:00.000Z");
  });
});
