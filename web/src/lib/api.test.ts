import { afterEach, describe, expect, it, vi } from "vitest";

import { getHealth, getHistory, getRuns } from "./api";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("getHealth", () => {
  it("decodes a healthy response", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({ service: "runloom", version: "0.1.0", status: "healthy" }),
            { status: 200 },
          ),
      ),
    );

    await expect(getHealth()).resolves.toEqual({
      service: "runloom",
      version: "0.1.0",
      status: "healthy",
    });
  });

  it("encodes project names and requests only selected history columns", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ runs: [] }), { status: 200 }))
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            run_id: "run-id",
            sequence: [],
            step: [],
            timestamp_ms: [],
            metrics: { loss: [] },
            next_after: null,
          }),
          { status: 200 },
        ),
      );
    vi.stubGlobal("fetch", fetchMock);

    await getRuns("robot learning");
    await getHistory("run-id", ["loss"], 500);

    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/projects/robot%20learning/runs?limit=200");
    expect(fetchMock.mock.calls[1][0]).toBe("/api/v1/runs/run-id/history?keys=loss&limit=500");
  });
});
