import { describe, expect, it, vi } from "vitest";

import { QueryScheduler } from "./query-scheduler";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("QueryScheduler", () => {
  it("bounds global concurrency and starts queued work as slots open", async () => {
    const scheduler = new QueryScheduler(2, 4);
    const gates = [deferred<number>(), deferred<number>(), deferred<number>()];
    const starts: number[] = [];
    const publish = vi.fn();
    gates.forEach((gate, index) =>
      scheduler.schedule({
        identity: `metric-${index}`,
        requestKey: "r1",
        request: async () => {
          starts.push(index);
          return gate.promise;
        },
        publish,
      }),
    );

    expect(starts).toEqual([0, 1]);
    gates[0].resolve(1);
    await vi.waitFor(() => expect(starts).toEqual([0, 1, 2]));
    gates[1].resolve(2);
    gates[2].resolve(3);
    await vi.waitFor(() => expect(publish).toHaveBeenCalledTimes(3));
  });

  it("cancels stale work for the same identity and publishes only the latest request", async () => {
    const scheduler = new QueryScheduler(1, 4);
    const oldGate = deferred<string>();
    const newGate = deferred<string>();
    const publish = vi.fn();
    const oldSignals: AbortSignal[] = [];
    scheduler.schedule({
      identity: "loss",
      requestKey: "revision-1",
      request: async (signal) => {
        oldSignals.push(signal);
        return oldGate.promise;
      },
      publish,
    });
    scheduler.schedule({
      identity: "loss",
      requestKey: "revision-2",
      request: () => newGate.promise,
      publish,
    });

    expect(oldSignals[0]?.aborted).toBe(true);
    expect(publish).not.toHaveBeenCalled();
    oldGate.resolve("old");
    await vi.waitFor(() => expect(oldSignals[0]?.aborted).toBe(true));
    newGate.resolve("new");
    await vi.waitFor(() => expect(publish).toHaveBeenCalledOnce());
    expect(publish).toHaveBeenCalledWith("new", "revision-2");
  });

  it("coalesces revision-only work behind an active request without aborting it", async () => {
    const scheduler = new QueryScheduler(1, 4);
    const active = deferred<string>();
    const superseded = deferred<string>();
    const newest = deferred<string>();
    const starts: string[] = [];
    const discarded: string[] = [];
    const signals: AbortSignal[] = [];
    const publish = vi.fn();
    const schedule = (requestKey: string, gate: Promise<string>) =>
      scheduler.schedule({
        identity: "loss",
        requestKey,
        schedulingPolicy: "coalesce-pending",
        request: async (signal) => {
          starts.push(requestKey);
          signals.push(signal);
          return gate;
        },
        publish,
        discard: () => discarded.push(requestKey),
      });

    schedule("revision-1", active.promise);
    schedule("revision-2", superseded.promise);
    schedule("revision-3", newest.promise);

    expect(starts).toEqual(["revision-1"]);
    expect(signals[0]?.aborted).toBe(false);
    expect(discarded).toEqual(["revision-2"]);

    active.resolve("active");
    await vi.waitFor(() => expect(starts).toEqual(["revision-1", "revision-3"]));
    expect(publish).toHaveBeenCalledWith("active", "revision-1");
    expect(signals[0]?.aborted).toBe(false);

    newest.resolve("newest");
    await vi.waitFor(() => expect(publish).toHaveBeenCalledTimes(2));
    expect(publish).toHaveBeenLastCalledWith("newest", "revision-3");
  });

  it("keeps an aborted request in its physical slot until the promise settles", async () => {
    const scheduler = new QueryScheduler(1, 4);
    const stale = deferred<string>();
    const replacement = deferred<string>();
    const starts: string[] = [];
    const publish = vi.fn();
    scheduler.schedule({
      identity: "loss",
      requestKey: "r1",
      request: async () => {
        starts.push("stale");
        return stale.promise;
      },
      publish,
    });
    scheduler.schedule({
      identity: "loss",
      requestKey: "r2",
      request: async () => {
        starts.push("replacement");
        return replacement.promise;
      },
      publish,
    });

    expect(starts).toEqual(["stale"]);
    stale.resolve("stale");
    await vi.waitFor(() => expect(starts).toEqual(["stale", "replacement"]));
    replacement.resolve("replacement");
    await vi.waitFor(() => expect(publish).toHaveBeenCalledOnce());
  });

  it("does not replace an active identity through an otherwise free slot", async () => {
    const scheduler = new QueryScheduler(2, 4);
    const stale = deferred<string>();
    const replacement = deferred<string>();
    const other = deferred<string>();
    const starts: string[] = [];
    const publish = vi.fn();
    const schedule = (identity: string, requestKey: string, label: string, gate: Promise<string>) =>
      scheduler.schedule({
        identity,
        requestKey,
        request: async () => {
          starts.push(label);
          return gate;
        },
        publish,
      });

    schedule("loss", "r1", "stale", stale.promise);
    schedule("loss", "r2", "replacement", replacement.promise);
    schedule("reward", "r1", "other", other.promise);
    expect(starts).toEqual(["stale", "other"]);

    stale.resolve("stale");
    await vi.waitFor(() => expect(starts).toEqual(["stale", "other", "replacement"]));
    replacement.resolve("replacement");
    other.resolve("other");
    await vi.waitFor(() => expect(publish).toHaveBeenCalledTimes(2));
  });

  it("queues the same request again when visibility returns before cancellation settles", async () => {
    const scheduler = new QueryScheduler(1, 4);
    const hidden = deferred<string>();
    const visible = deferred<string>();
    const starts: string[] = [];
    const publish = vi.fn();
    const query = (request: () => Promise<string>) => ({
      identity: "loss",
      requestKey: "same-revision",
      request,
      publish,
    });
    scheduler.schedule(
      query(async () => {
        starts.push("hidden");
        return hidden.promise;
      }),
    );
    scheduler.cancel("loss");
    scheduler.schedule(
      query(async () => {
        starts.push("visible");
        return visible.promise;
      }),
    );

    expect(starts).toEqual(["hidden"]);
    hidden.resolve("hidden");
    await vi.waitFor(() => expect(starts).toEqual(["hidden", "visible"]));
    visible.resolve("visible");
    await vi.waitFor(() => expect(publish).toHaveBeenCalledOnce());
    expect(publish).toHaveBeenCalledWith("visible", "same-revision");
  });

  it("discards the oldest pending query when the explicit queue bound is reached", async () => {
    const scheduler = new QueryScheduler(1, 2);
    const active = deferred<string>();
    const pending = [deferred<string>(), deferred<string>(), deferred<string>()];
    const starts: string[] = [];
    const discarded: string[] = [];
    const publish = vi.fn();
    scheduler.schedule({
      identity: "active",
      requestKey: "r1",
      request: async () => {
        starts.push("active");
        return active.promise;
      },
      publish,
    });
    pending.forEach((gate, index) =>
      scheduler.schedule({
        identity: `pending-${index}`,
        requestKey: "r1",
        request: async () => {
          starts.push(`pending-${index}`);
          return gate.promise;
        },
        publish,
        discard: () => discarded.push(`pending-${index}`),
      }),
    );

    expect(discarded).toEqual(["pending-0"]);
    active.resolve("active");
    await vi.waitFor(() => expect(starts).toEqual(["active", "pending-1"]));
    pending[1].resolve("second");
    await vi.waitFor(() => expect(starts).toEqual(["active", "pending-1", "pending-2"]));
    pending[2].resolve("third");
    await vi.waitFor(() => expect(publish).toHaveBeenCalledTimes(3));
  });
});
