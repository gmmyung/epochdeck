import { afterEach, describe, expect, it, vi } from "vitest";

import { getHealth } from "./api";

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
});
