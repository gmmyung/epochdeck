import { describe, expect, it, vi } from "vitest";

import { BoundedRequestScheduler } from "./bounded-request-scheduler";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("BoundedRequestScheduler", () => {
  it("bounds physical concurrency and the pending queue", async () => {
    const scheduler = new BoundedRequestScheduler(2, 2);
    const gates = Array.from({ length: 5 }, () => deferred<number>());
    const starts: number[] = [];
    const signal = new AbortController().signal;
    const results = gates.map((gate, index) =>
      scheduler.run({
        identity: `detail-${index}`,
        parentSignal: signal,
        request: async () => {
          starts.push(index);
          return gate.promise;
        },
      }),
    );

    expect(starts).toEqual([]);
    await vi.waitFor(() => expect(starts).toEqual([0, 1]));
    await expect(results[2]).resolves.toBeUndefined();

    gates[0].resolve(0);
    await vi.waitFor(() => expect(starts).toEqual([0, 1, 3]));
    gates[1].resolve(1);
    gates[3].resolve(3);
    gates[4].resolve(4);
    await expect(Promise.all(results)).resolves.toEqual([0, 1, undefined, 3, 4]);
  });

  it("keeps an aborted physical request in its slot until it settles", async () => {
    const scheduler = new BoundedRequestScheduler(1, 2);
    const stale = deferred<string>();
    const replacement = deferred<string>();
    const staleParent = new AbortController();
    const starts: string[] = [];
    const staleResult = scheduler.run({
      identity: "stale",
      parentSignal: staleParent.signal,
      request: async () => {
        starts.push("stale");
        return stale.promise;
      },
    });
    const replacementResult = scheduler.run({
      identity: "replacement",
      parentSignal: new AbortController().signal,
      request: async () => {
        starts.push("replacement");
        return replacement.promise;
      },
    });

    await vi.waitFor(() => expect(starts).toEqual(["stale"]));
    staleParent.abort();
    expect(starts).toEqual(["stale"]);
    stale.resolve("ignored");
    await vi.waitFor(() => expect(starts).toEqual(["stale", "replacement"]));
    replacement.resolve("fresh");

    await expect(staleResult).resolves.toBeUndefined();
    await expect(replacementResult).resolves.toBe("fresh");
  });
});
