import { describe, expect, it, vi } from "vitest";

import { VideoAutoplayCoordinator } from "./video-autoplay";

function video() {
  return { play: vi.fn(async () => undefined), pause: vi.fn() };
}

describe("video autoplay coordinator", () => {
  it("plays only the most visible eligible video", () => {
    const first = video();
    const second = video();
    const coordinator = new VideoAutoplayCoordinator();
    coordinator.register(first);
    coordinator.register(second);

    coordinator.visibility(first, 0.6, true);
    coordinator.visibility(second, 0.8, true);

    expect(first.play).toHaveBeenCalledOnce();
    expect(first.pause).toHaveBeenCalledOnce();
    expect(second.play).toHaveBeenCalledOnce();
  });

  it("disables autoplay when the user or network policy asks for it", () => {
    const candidate = video();
    let allowed = true;
    const coordinator = new VideoAutoplayCoordinator(() => allowed);
    coordinator.register(candidate);
    coordinator.visibility(candidate, 1, true);
    allowed = false;
    coordinator.refresh();

    expect(candidate.play).toHaveBeenCalledOnce();
    expect(candidate.pause).toHaveBeenCalledOnce();
  });
});
