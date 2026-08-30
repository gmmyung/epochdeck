export type BoundedRequest<T> = {
  identity: string;
  parentSignal: AbortSignal;
  request: (signal: AbortSignal) => Promise<T>;
};

type PendingRequest = {
  identity: string;
  parentSignal: AbortSignal;
  request: (signal: AbortSignal) => Promise<unknown>;
  promise: Promise<unknown | undefined>;
  resolve: (value: unknown | undefined) => void;
  reject: (reason: unknown) => void;
  removeAbortListener: () => void;
};

type ActiveRequest = {
  task: PendingRequest;
  controller: AbortController;
};

/** Bounds both physical request concurrency and the latest-first pending queue. */
export class BoundedRequestScheduler {
  private readonly active = new Map<string, ActiveRequest>();
  private readonly pending = new Map<string, PendingRequest>();

  constructor(
    private readonly concurrency: number,
    private readonly maximumPending: number,
  ) {
    if (!Number.isInteger(concurrency) || concurrency < 1) {
      throw new RangeError("request concurrency must be a positive integer");
    }
    if (!Number.isInteger(maximumPending) || maximumPending < 1) {
      throw new RangeError("maximum pending requests must be a positive integer");
    }
  }

  run<T>(request: BoundedRequest<T>): Promise<T | undefined> {
    if (request.parentSignal.aborted) return Promise.resolve(undefined);
    const existing =
      this.pending.get(request.identity)?.promise ??
      this.active.get(request.identity)?.task.promise;
    if (existing) return existing as Promise<T | undefined>;

    let resolve!: (value: unknown | undefined) => void;
    let reject!: (reason: unknown) => void;
    const promise = new Promise<unknown | undefined>((accept, fail) => {
      resolve = accept;
      reject = fail;
    });
    const task: PendingRequest = {
      ...request,
      promise,
      resolve,
      reject,
      removeAbortListener: () => {},
    };
    const abort = () => this.abort(task);
    request.parentSignal.addEventListener("abort", abort, { once: true });
    task.removeAbortListener = () => request.parentSignal.removeEventListener("abort", abort);

    this.evictPendingForCapacity();
    this.pending.set(request.identity, task);
    this.drain();
    return promise as Promise<T | undefined>;
  }

  cancelAll(): void {
    for (const task of this.pending.values()) this.finishPending(task);
    this.pending.clear();
    for (const request of this.active.values()) request.controller.abort();
  }

  get activeCount(): number {
    return this.active.size;
  }

  get pendingCount(): number {
    return this.pending.size;
  }

  private abort(task: PendingRequest): void {
    if (this.pending.get(task.identity) === task) {
      this.pending.delete(task.identity);
      this.finishPending(task);
      this.drain();
      return;
    }
    const active = this.active.get(task.identity);
    if (active?.task === task) active.controller.abort();
  }

  private evictPendingForCapacity(): void {
    if (this.pending.size < this.maximumPending) return;
    const oldest = this.pending.values().next().value as PendingRequest | undefined;
    if (!oldest) return;
    this.pending.delete(oldest.identity);
    this.finishPending(oldest);
  }

  private finishPending(task: PendingRequest): void {
    task.removeAbortListener();
    task.resolve(undefined);
  }

  private drain(): void {
    while (this.active.size < this.concurrency && this.pending.size > 0) {
      const next = this.pending.entries().next().value as [string, PendingRequest] | undefined;
      if (!next) return;
      const [identity, task] = next;
      this.pending.delete(identity);
      if (task.parentSignal.aborted) {
        this.finishPending(task);
        continue;
      }
      const controller = new AbortController();
      const active = { task, controller };
      this.active.set(identity, active);
      void Promise.resolve()
        .then(() => task.request(controller.signal))
        .then(
          (value) => task.resolve(controller.signal.aborted ? undefined : value),
          (reason) => {
            if (controller.signal.aborted || task.parentSignal.aborted) task.resolve(undefined);
            else task.reject(reason);
          },
        )
        .finally(() => {
          task.removeAbortListener();
          if (this.active.get(identity) === active) this.active.delete(identity);
          this.drain();
        });
    }
  }
}
