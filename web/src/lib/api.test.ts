import { afterEach, describe, expect, it, vi } from "vitest";

import {
  artifactArchiveUrl,
  blobUrl,
  artifactFileUrl,
  comparisonSeriesHistory,
  getDashboardConfig,
  getAlertPage,
  getArtifact,
  getChartHistory,
  getComparisonChartHistory,
  getHealth,
  getProjectMetricCatalogPage,
  getProject,
  getProjectPage,
  getReport,
  getReportPage,
  getRichValue,
  getRichValueKeyPage,
  getRichValuePage,
  getRun,
  getRunArtifactPage,
  getRunPage,
  getRunSummariesByIds,
  getTrace,
  getTracePage,
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
            JSON.stringify({ service: "epochdeck", version: "0.1.0", status: "healthy" }),
            { status: 200 },
          ),
      ),
    );

    await expect(getHealth()).resolves.toEqual({
      service: "epochdeck",
      version: "0.1.0",
      status: "healthy",
    });
  });

  it("loads server dashboard branding", async () => {
    const fetchMock = vi.fn(async () =>
      Response.json({
        logo_url: "/api/v1/dashboard/logo",
        favicon_url: "/api/v1/dashboard/favicon?v=1234",
        accent_color: "#8a31c7",
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(getDashboardConfig()).resolves.toEqual({
      logo_url: "/api/v1/dashboard/logo",
      favicon_url: "/api/v1/dashboard/favicon?v=1234",
      accent_color: "#8a31c7",
    });
    expect(fetchMock).toHaveBeenCalledWith("/api/v1/dashboard/config", { signal: undefined });
  });

  it("encodes project names", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ runs: [], next_before: null }), { status: 200 }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await getRunPage("robot learning");

    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/projects/robot%20learning/runs?limit=100");
  });

  it("loads cursor pages for projects, searched runs, and report summaries", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ projects: [], next_before: "project-cursor" }), {
          status: 200,
        }),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            id: "project/id",
            name: "robot learning",
            mutation_token: "900719925474099312345",
          }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ runs: [], next_before: "run-cursor" }), { status: 200 }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ reports: [], next_before: "report-cursor" }), {
          status: 200,
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ id: "report/id", layout: { columns: 1, panels: [] } }), {
          status: 200,
        }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(getProjectPage("project/id")).resolves.toEqual({
      items: [],
      nextBefore: "project-cursor",
    });
    await expect(getProject("robot learning")).resolves.toMatchObject({
      id: "project/id",
      name: "robot learning",
      mutation_token: "900719925474099312345",
    });
    await expect(getRunPage("robot learning", " reward + bonus ", "run/id")).resolves.toEqual({
      items: [],
      nextBefore: "run-cursor",
    });
    await expect(getReportPage("robot learning", "report/id")).resolves.toEqual({
      items: [],
      nextBefore: "report-cursor",
    });
    await expect(getReport("report/id")).resolves.toMatchObject({ id: "report/id" });
    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([
      "/api/v1/projects?limit=100&before=project%2Fid",
      "/api/v1/projects/robot%20learning",
      "/api/v1/projects/robot%20learning/runs?limit=100&before=run%2Fid&q=reward+%2B+bonus",
      "/api/v1/projects/robot%20learning/reports?limit=100&before=report%2Fid",
      "/api/v1/reports/report%2Fid",
    ]);
  });

  it("loads one run", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ id: "run-id", metric_revision: 4 }), { status: 200 }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await getRun("run-id");

    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/runs/run-id");
  });

  it("polls an exact bounded run set with lightweight summaries", async () => {
    const fetchMock = vi.fn().mockResolvedValueOnce(
      new Response(JSON.stringify({ runs: [{ id: "run-a" }], next_before: null }), {
        status: 200,
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(getRunSummariesByIds("robot learning", ["run-a", "run-b"])).resolves.toEqual([
      { id: "run-a" },
    ]);
    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/query/runs");
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      project: "robot learning",
      run_ids: ["run-a", "run-b"],
      limit: 2,
    });
  });

  it("queries one bounded project metric-catalog page", async () => {
    const fetchMock = vi.fn().mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          keys: [{ key: "comma,key", run_ids: ["run/a"] }],
          next_after: "loss",
          total_count: 37,
        }),
        { status: 200 },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      getProjectMetricCatalogPage(
        "robot learning",
        ["run/a", "run/b"],
        "intersection",
        " reward + bonus ",
        "loss",
        24,
      ),
    ).resolves.toEqual({
      items: [{ key: "comma,key", run_ids: ["run/a"] }],
      nextAfter: "loss",
      totalCount: 37,
    });
    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/projects/robot%20learning/metrics/query");
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      run_ids: ["run/a", "run/b"],
      mode: "intersection",
      search: "reward + bonus",
      after: "loss",
      limit: 24,
    });
  });

  it("surfaces the server error code and message", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(JSON.stringify({ code: "invalid_request", message: "bad metric" }), {
            status: 422,
          }),
      ),
    );

    await expect(getHealth()).rejects.toMatchObject({
      name: "EpochDeckApiError",
      status: 422,
      code: "invalid_request",
      message: "bad metric",
    });
  });

  it("rejects oversized UTF-8 searches before issuing a request", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);

    await expect(getRunPage("demo", "한".repeat(100))).rejects.toThrow(/256 non-control bytes/);
    await expect(
      getProjectMetricCatalogPage("demo", ["run-a"], "union", "loss\nsecret"),
    ).rejects.toThrow(/256 non-control bytes/);
    await expect(getTracePage("run-a", "한".repeat(100))).rejects.toThrow(/256 non-control bytes/);
    await expect(getRunPage("demo", "loss\u0085hidden")).rejects.toThrow(/256 non-control bytes/);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("requests exact chart buckets for encoded metric keys and a paired viewport", async () => {
    const chartHistory = {
      run_id: "run/id",
      step_min: 10,
      step_max: 20,
      bucket_count: 512,
      source_points: 4,
      source_last_sequence: 9,
      metrics: {
        "train/loss": {
          source_points: 4,
          bucket: [0, 511],
          last_x: [10, 20],
          last_step: [10, 20],
          last_timestamp_ms: [100, 200],
          minimum: [0.5, 0.25],
          maximum: [1, 0.75],
          last: [0.75, 0.5],
        },
      },
    };
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(chartHistory), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      getChartHistory("run/id", ["train/loss", "comma,key", "reward + bonus"], {
        maxBuckets: 512,
        viewport: { stepMin: 10, stepMax: 20 },
      }),
    ).resolves.toEqual(chartHistory);
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/v1/runs/run%2Fid/chart-history?key=train%2Floss&key=comma%2Ckey&key=reward+%2B+bonus&max_buckets=512&step_min=10&step_max=20",
    );
  });

  it("posts a bounded multi-metric comparison and maps one series for the chart", async () => {
    const comparison = {
      project: "robot learning",
      alignment: "relative_step",
      x_min: 0,
      x_max: 10,
      bucket_count: 256,
      runs: [{ run_id: "run-a", source_last_sequence: 42 }],
      series: [
        {
          run_id: "run-a",
          key: "train/loss",
          source_points: 2,
          bucket: [0, 255],
          last_x: [0, 10],
          last_step: [100, 110],
          last_timestamp_ms: [1_000, 2_000],
          minimum: [0.5, 0.25],
          maximum: [1, 0.5],
          last: [0.75, 0.3],
        },
      ],
    };
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(comparison), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const result = await getComparisonChartHistory(
      "robot learning",
      [
        { run_id: "run-a", key: "train/loss" },
        { run_id: "run-b", key: "train/loss" },
        { run_id: "run-a", key: "train/reward" },
      ],
      {
        alignment: "relative_step",
        maxBuckets: 256,
        viewport: { minimum: 0, maximum: 10 },
      },
    );

    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/v1/projects/robot%20learning/chart-history/query",
    );
    expect(fetchMock.mock.calls[0][1]).toMatchObject({
      method: "POST",
      headers: { "content-type": "application/json" },
    });
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      series: [
        { run_id: "run-a", key: "train/loss" },
        { run_id: "run-b", key: "train/loss" },
        { run_id: "run-a", key: "train/reward" },
      ],
      alignment: "relative_step",
      max_buckets: 256,
      viewport: { minimum: 0, maximum: 10 },
    });
    expect(comparisonSeriesHistory(result, "run-a", "train/loss")).toMatchObject({
      run_id: "run-a",
      step_min: 0,
      step_max: 10,
      source_last_sequence: 42,
      metrics: { "train/loss": { last_x: [0, 10], last_step: [100, 110] } },
    });
  });

  it("loads a bounded alert page", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ alerts: [], next_before: null }), { status: 200 }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(getAlertPage("run/id", "alert/id")).resolves.toEqual({
      items: [],
      nextBefore: null,
    });
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/v1/runs/run%2Fid/alerts?limit=100&before=alert%2Fid",
    );
  });

  it("loads the rich-key catalog, one keyed summary page, and selected detail", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ keys: [], next_after: "train/video" }), { status: 200 }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ values: [], next_before: "value/id" }), { status: 200 }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ id: "value/id", metadata: { caption: "hello" } }), {
          status: 200,
        }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(getRichValueKeyPage("run/id", "old/key")).resolves.toEqual({
      items: [],
      nextAfter: "train/video",
    });
    await expect(getRichValuePage("run/id", "train/video", "value/id")).resolves.toEqual({
      items: [],
      nextBefore: "value/id",
    });
    await expect(getRichValue("value/id")).resolves.toMatchObject({
      id: "value/id",
      metadata: { caption: "hello" },
    });
    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([
      "/api/v1/runs/run%2Fid/rich-values/keys?limit=100&after=old%2Fkey",
      "/api/v1/runs/run%2Fid/rich-values?limit=100&before=value%2Fid&key=train%2Fvideo",
      "/api/v1/rich-values/value%2Fid",
    ]);
    expect(blobUrl({ digest: "abc", size: 3, mime_type: "video/mp4", file_name: null })).toBe(
      "/api/v1/blobs/abc?mime=video%2Fmp4",
    );
  });

  it("loads artifact summaries with a paired cursor and selected detail", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            artifacts: [],
            next_before: "artifact/id",
            next_before_relation: "input",
          }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ id: "artifact/id", entries: [] }), { status: 200 }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      getRunArtifactPage("run/id", { before: "previous/id", relation: "output" }),
    ).resolves.toEqual({
      items: [],
      nextCursor: { before: "artifact/id", relation: "input" },
    });
    await expect(getArtifact("artifact/id")).resolves.toMatchObject({ id: "artifact/id" });
    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([
      "/api/v1/runs/run%2Fid/artifacts?limit=100&before=previous%2Fid&before_relation=output",
      "/api/v1/artifacts/artifact%2Fid",
    ]);
    expect(artifactFileUrl("artifact/id", "checkpoints/best model.bin")).toBe(
      "/api/v1/artifacts/artifact%2Fid/files/checkpoints/best%20model.bin",
    );
    expect(artifactArchiveUrl("artifact/id")).toBe("/api/v1/artifacts/artifact%2Fid/download");
  });

  it("loads trace summaries with an encoded query and selected detail", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ spans: [], next_before: "span/id" }), { status: 200 }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ id: "span/id", attributes: {}, preview: {} }), {
          status: 200,
        }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(getTracePage("run/id", " assistant reward ", "span/id")).resolves.toEqual({
      items: [],
      nextBefore: "span/id",
    });
    await expect(getTrace("span/id")).resolves.toMatchObject({ id: "span/id" });
    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([
      "/api/v1/runs/run%2Fid/traces?limit=100&q=assistant+reward&before=span%2Fid",
      "/api/v1/traces/span%2Fid",
    ]);
  });
});
