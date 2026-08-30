import { afterEach, describe, expect, it, vi } from "vitest";

import { LiveRefreshCoordinator } from "./live-refresh-coordinator";

afterEach(() => {
  vi.useRealTimers();
});

describe("LiveRefreshCoordinator", () => {
  it("dispatches immediately, then coalesces invalidations to the latest task", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000);
    const coordinator = new LiveRefreshCoordinator(10_000, 2);
    const calls: string[] = [];

    coordinator.invalidate("comparison", () => calls.push("first"));
    vi.setSystemTime(3_000);
    coordinator.invalidate("comparison", () => calls.push("stale"));
    vi.setSystemTime(5_000);
    coordinator.invalidate("comparison", () => calls.push("latest"));

    expect(calls).toEqual(["first"]);
    expect(coordinator.pendingCount).toBe(1);
    await vi.advanceTimersByTimeAsync(5_999);
    expect(calls).toEqual(["first"]);
    await vi.advanceTimersByTimeAsync(1);
    expect(calls).toEqual(["first", "latest"]);
    expect(coordinator.pendingCount).toBe(0);
  });

  it("lets a final refresh bypass the cooldown without leaving stale work queued", () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000);
    const coordinator = new LiveRefreshCoordinator(10_000, 2);
    const calls: string[] = [];

    coordinator.invalidate("report", () => calls.push("running"));
    vi.setSystemTime(2_000);
    coordinator.invalidate("report", () => calls.push("intermediate"));
    coordinator.invalidate("report", () => calls.push("finished"), true);

    expect(calls).toEqual(["running", "finished"]);
    expect(coordinator.pendingCount).toBe(0);
  });

  it("forgets pending work and cooldown state when navigation changes", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000);
    const coordinator = new LiveRefreshCoordinator(10_000, 2);
    const calls: string[] = [];

    coordinator.invalidate("comparison", () => calls.push("first"));
    vi.setSystemTime(2_000);
    coordinator.invalidate("comparison", () => calls.push("stale"));
    coordinator.forget("comparison");
    coordinator.invalidate("comparison", () => calls.push("new-view"));
    await vi.runAllTimersAsync();

    expect(calls).toEqual(["first", "new-view"]);
    expect(coordinator.pendingCount).toBe(0);
  });

  it("bounds remembered and pending refresh identities", () => {
    vi.useFakeTimers();
    const coordinator = new LiveRefreshCoordinator(10_000, 2);

    coordinator.invalidate("comparison", () => {});
    coordinator.invalidate("report", () => {});

    expect(() => coordinator.invalidate("third", () => {})).toThrow(
      "live refresh identity limit of 2 exceeded",
    );
    coordinator.forget("comparison");
    expect(() => coordinator.invalidate("third", () => {})).not.toThrow();
  });
});
