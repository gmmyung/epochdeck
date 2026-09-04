type LiveRefreshTask = () => void;

type TimerHandle = ReturnType<typeof globalThis.setTimeout>;

export class LiveRefreshCoordinator {
  private readonly pending = new Map<string, { task: LiveRefreshTask; urgent: boolean }>();
  private readonly lastDispatch = new Map<string, number>();
  private timer: TimerHandle | null = null;

  constructor(
    private readonly cooldownMs: number,
    private readonly maxIdentities: number,
  ) {
    if (!Number.isFinite(cooldownMs) || cooldownMs < 0) {
      throw new Error("live refresh cooldown must be a non-negative number");
    }
    if (!Number.isSafeInteger(maxIdentities) || maxIdentities < 1) {
      throw new Error("live refresh identity limit must be a positive safe integer");
    }
  }

  invalidate(identity: string, task: LiveRefreshTask, urgent = false): void {
    if (!this.pending.has(identity) && !this.lastDispatch.has(identity)) {
      const knownIdentities = new Set([...this.pending.keys(), ...this.lastDispatch.keys()]);
      if (knownIdentities.size >= this.maxIdentities) {
        throw new Error(`live refresh identity limit of ${this.maxIdentities} exceeded`);
      }
    }
    const existing = this.pending.get(identity);
    this.pending.delete(identity);
    this.pending.set(identity, { task, urgent: urgent || existing?.urgent === true });
    this.drain();
  }

  forget(identity: string): void {
    this.pending.delete(identity);
    this.lastDispatch.delete(identity);
    this.scheduleNext();
  }

  clear(): void {
    this.pending.clear();
    this.lastDispatch.clear();
    this.cancelTimer();
  }

  private drain(): void {
    this.cancelTimer();
    const now = Date.now();
    const ready: LiveRefreshTask[] = [];
    for (const [identity, pending] of this.pending) {
      const lastDispatch = this.lastDispatch.get(identity);
      const dueAt = pending.urgent
        ? now
        : lastDispatch === undefined
          ? now
          : lastDispatch + this.cooldownMs;
      if (dueAt > now) continue;
      this.pending.delete(identity);
      this.lastDispatch.set(identity, now);
      ready.push(pending.task);
    }
    this.scheduleNext();
    for (const task of ready) task();
  }

  private scheduleNext(): void {
    this.cancelTimer();
    if (this.pending.size === 0) return;
    const now = Date.now();
    let nextDueAt = Number.POSITIVE_INFINITY;
    for (const [identity, pending] of this.pending) {
      const lastDispatch = this.lastDispatch.get(identity);
      const dueAt = pending.urgent
        ? now
        : lastDispatch === undefined
          ? now
          : lastDispatch + this.cooldownMs;
      nextDueAt = Math.min(nextDueAt, dueAt);
    }
    this.timer = globalThis.setTimeout(
      () => {
        this.timer = null;
        this.drain();
      },
      Math.max(0, nextDueAt - now),
    );
  }

  private cancelTimer(): void {
    if (this.timer === null) return;
    globalThis.clearTimeout(this.timer);
    this.timer = null;
  }
}
