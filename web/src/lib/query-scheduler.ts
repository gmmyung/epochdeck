type ScheduledQuery<T> = {
  identity: string;
  requestKey: string;
  schedulingPolicy?: "abort-active" | "coalesce-pending";
  request: (signal: AbortSignal) => Promise<T>;
  publish: (value: T, requestKey: string) => void;
  reject?: (reason: unknown) => void;
  discard?: () => void;
};

type ActiveQuery = {
  requestKey: string;
  controller: AbortController;
};

export class QueryScheduler {
  private readonly pending = new Map<string, ScheduledQuery<unknown>>();
  private readonly active = new Map<string, ActiveQuery>();

  constructor(
    private readonly concurrency: number,
    private readonly maximumPending: number,
  ) {
    if (!Number.isInteger(concurrency) || concurrency < 1) {
      throw new Error("query concurrency must be a positive integer");
    }
    if (!Number.isInteger(maximumPending) || maximumPending < 1) {
      throw new Error("maximum pending queries must be a positive integer");
    }
  }

  schedule<T>(query: ScheduledQuery<T>): void {
    const active = this.active.get(query.identity);
    if (active?.requestKey === query.requestKey && !active.controller.signal.aborted) return;
    if (this.pending.get(query.identity)?.requestKey === query.requestKey) return;
    if (active && query.schedulingPolicy !== "coalesce-pending") {
      active.controller.abort();
    }
    this.discardPending(query.identity);
    if (this.pending.size >= this.maximumPending) {
      const oldestIdentity = this.pending.keys().next().value as string | undefined;
      if (oldestIdentity !== undefined) this.discardPending(oldestIdentity);
    }
    this.pending.set(query.identity, query as ScheduledQuery<unknown>);
    this.drain();
  }

  cancel(identity: string): void {
    this.discardPending(identity);
    this.active.get(identity)?.controller.abort();
  }

  cancelAll(): void {
    for (const identity of this.pending.keys()) this.discardPending(identity);
    for (const query of this.active.values()) query.controller.abort();
  }

  private discardPending(identity: string): void {
    const query = this.pending.get(identity);
    if (!query) return;
    this.pending.delete(identity);
    query.discard?.();
  }

  private drain(): void {
    while (this.active.size < this.concurrency && this.pending.size > 0) {
      const next = [...this.pending.entries()].find(([identity]) => !this.active.has(identity));
      if (!next) return;
      const [identity, query] = next;
      this.pending.delete(identity);
      const controller = new AbortController();
      const active = { requestKey: query.requestKey, controller };
      this.active.set(identity, active);
      void query
        .request(controller.signal)
        .then((result) => {
          if (this.active.get(identity) !== active || controller.signal.aborted) return;
          query.publish(result, query.requestKey);
        })
        .catch((reason) => {
          if (!controller.signal.aborted) query.reject?.(reason);
        })
        .finally(() => {
          if (this.active.get(identity) === active) this.active.delete(identity);
          this.drain();
        });
    }
  }
}
