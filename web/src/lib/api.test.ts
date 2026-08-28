import { afterEach, describe, expect, it, vi } from "vitest";

import { getAlerts, getHealth, getHistory, getRun, getRuns, getSampledHistory } from "./api";

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
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            run_id: "run-id",
            sequence: [],
            step: [],
            timestamp_ms: [],
            metrics: { loss: [] },
            next_after: null,
            sampled: true,
            source_points: 200_000,
          }),
          { status: 200 },
        ),
      );
    vi.stubGlobal("fetch", fetchMock);

    await getRuns("robot learning");
    await getHistory("run-id", ["loss"], 500);
    await getSampledHistory("run-id", ["loss"], 1_200);

    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/projects/robot%20learning/runs?limit=200");
    expect(fetchMock.mock.calls[1][0]).toBe("/api/v1/runs/run-id/history?keys=loss&limit=500");
    expect(fetchMock.mock.calls[2][0]).toBe(
      "/api/v1/runs/run-id/history?keys=loss&max_points=1200",
    );
  });

  it("loads one run and encodes a bounded delta cursor", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ id: "run-id", metric_revision: 4 }), { status: 200 }),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            run_id: "run-id",
            sequence: [],
            step: [],
            timestamp_ms: [],
            metrics: { loss: [] },
            next_after: null,
            source_last_sequence: 42,
          }),
          { status: 200 },
        ),
      );
    vi.stubGlobal("fetch", fetchMock);

    await getRun("run-id");
    await getHistory("run-id", ["loss"], 257, undefined, 42);

    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/runs/run-id");
    expect(fetchMock.mock.calls[1][0]).toBe(
      "/api/v1/runs/run-id/history?keys=loss&limit=257&after=42",
    );
  });

  it("loads a bounded alert page", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ alerts: [], next_before: null }), { status: 200 }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(getAlerts("run/id")).resolves.toEqual([]);
    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/runs/run%2Fid/alerts?limit=100");
  });
});
