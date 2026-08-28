import { afterEach, describe, expect, it, vi } from "vitest";

import {
  blobUrl,
  artifactFileUrl,
  getAlerts,
  getHealth,
  getHistory,
  getReports,
  getRichValues,
  getRunArtifacts,
  getRun,
  getRuns,
  getSampledHistory,
  getTraces,
} from "./api";

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

  it("loads a bounded report collection for an encoded project", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ reports: [] }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(getReports("robot learning")).resolves.toEqual([]);
    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/projects/robot%20learning/reports?limit=100");
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

  it("loads bounded rich values and builds an encoded blob URL", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ values: [], next_before: null }), { status: 200 }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(getRichValues("run/id")).resolves.toEqual([]);
    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/runs/run%2Fid/rich-values?limit=100");
    expect(blobUrl({ digest: "abc", size: 3, mime_type: "video/mp4", file_name: null })).toBe(
      "/api/v1/blobs/abc?mime=video%2Fmp4",
    );
  });

  it("loads run artifact lineage and encodes nested file paths", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ artifacts: [] }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(getRunArtifacts("run/id")).resolves.toEqual([]);
    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/runs/run%2Fid/artifacts");
    expect(artifactFileUrl("artifact/id", "checkpoints/best model.bin")).toBe(
      "/api/v1/artifacts/artifact%2Fid/files/checkpoints/best%20model.bin",
    );
  });

  it("loads bounded traces with an encoded full-text query", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ spans: [], next_before: null }), { status: 200 }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(getTraces("run/id", " assistant reward ")).resolves.toEqual([]);
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/v1/runs/run%2Fid/traces?limit=100&q=assistant+reward",
    );
  });
});
