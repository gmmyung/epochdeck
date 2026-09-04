type PlayableVideo = {
  play: () => Promise<void>;
  pause: () => void;
};

export class VideoAutoplayCoordinator<T extends PlayableVideo> {
  private readonly candidates = new Map<T, number>();
  private active: T | null = null;

  constructor(private readonly allowed: () => boolean = () => true) {}

  register(video: T): () => void {
    this.candidates.set(video, 0);
    return () => {
      this.candidates.delete(video);
      if (this.active === video) {
        video.pause();
        this.active = null;
      }
      this.refresh();
    };
  }

  visibility(video: T, ratio: number, intersecting: boolean): void {
    if (!this.candidates.has(video)) return;
    this.candidates.set(video, intersecting ? Math.max(0, Math.min(ratio, 1)) : 0);
    this.refresh();
  }

  refresh(): void {
    let next: T | null = null;
    let bestRatio = 0.45;
    if (this.allowed()) {
      for (const [candidate, ratio] of this.candidates) {
        if (ratio < bestRatio) continue;
        bestRatio = ratio;
        next = candidate;
      }
    }
    if (next === this.active) return;
    this.active?.pause();
    this.active = next;
    if (next) void next.play().catch(() => undefined);
  }
}

const browserCoordinator = new VideoAutoplayCoordinator<HTMLVideoElement>(browserAutoplayAllowed);
let browserRegistrations = 0;
let reducedMotionQuery: MediaQueryList | null = null;

export function autoplayVideoWhenVisible(video: HTMLVideoElement): { destroy: () => void } {
  video.muted = true;
  const unregister = browserCoordinator.register(video);
  const observer = new IntersectionObserver(
    ([entry]) => {
      browserCoordinator.visibility(
        video,
        entry?.intersectionRatio ?? 0,
        entry?.isIntersecting ?? false,
      );
    },
    { threshold: [0, 0.45, 0.6, 0.75, 0.9, 1] },
  );
  observer.observe(video);
  browserRegistrations += 1;
  if (browserRegistrations === 1) {
    document.addEventListener("visibilitychange", refreshBrowserCoordinator);
    reducedMotionQuery = window.matchMedia("(prefers-reduced-motion: reduce)");
    reducedMotionQuery.addEventListener("change", refreshBrowserCoordinator);
  }
  return {
    destroy: () => {
      observer.disconnect();
      unregister();
      video.pause();
      browserRegistrations = Math.max(0, browserRegistrations - 1);
      if (browserRegistrations === 0) {
        document.removeEventListener("visibilitychange", refreshBrowserCoordinator);
        reducedMotionQuery?.removeEventListener("change", refreshBrowserCoordinator);
        reducedMotionQuery = null;
      }
    },
  };
}

function refreshBrowserCoordinator(): void {
  browserCoordinator.refresh();
}

function browserAutoplayAllowed(): boolean {
  if (document.visibilityState !== "visible") return false;
  if ((reducedMotionQuery ?? window.matchMedia("(prefers-reduced-motion: reduce)")).matches) {
    return false;
  }
  const connection = (navigator as Navigator & { connection?: { saveData?: boolean } }).connection;
  return connection?.saveData !== true;
}
