export type ScheduledQuery<T> = {
  identity: string;
  requestKey: string;
  request: (signal: AbortSignal) => Promise<T>;
  publish: (value: T, requestKey: string) => void;
  reject?: (reason: unknown) => void;
};

type ActiveQuery = {
  requestKey: string;
  controller: AbortController;
};

export class QueryScheduler {
  private readonly pending = new Map<string, ScheduledQuery<unknown>>();
  private readonly active = new Map<string, ActiveQuery>();

  constructor(private readonly concurrency: number) {
    if (!Number.isInteger(concurrency) || concurrency < 1) {
      throw new Error("query concurrency must be a positive integer");
    }
  }

  schedule<T>(query: ScheduledQuery<T>): void {
    const active = this.active.get(query.identity);
    if (active?.requestKey === query.requestKey && !active.controller.signal.aborted) return;
    if (this.pending.get(query.identity)?.requestKey === query.requestKey) return;
    if (active) {
      active.controller.abort();
    }
    this.pending.delete(query.identity);
    this.pending.set(query.identity, query as ScheduledQuery<unknown>);
    this.drain();
  }

  cancel(identity: string): void {
    this.pending.delete(identity);
    this.active.get(identity)?.controller.abort();
  }

  cancelAll(): void {
    this.pending.clear();
    for (const query of this.active.values()) query.controller.abort();
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
