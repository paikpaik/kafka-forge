export interface IdempotencyStore {
  wasProcessed(key: string): Promise<boolean>;
  markProcessed(key: string): Promise<void>;
}

export interface InMemoryIdempotencyStoreOptions {
  /** 이 시간(ms)이 지난 키는 처리 안 한 것으로 취급한다. 생략하면 키가 영구히 쌓인다 — 오래 도는 프로세스라면 반드시 지정할 것. */
  ttlMs?: number;
  /** 만료된 키를 실제로 메모리에서 제거하는 주기(ms). 기본값은 ttlMs와 동일. */
  sweepIntervalMs?: number;
}

/**
 * 학습/데모/테스트용 기본 구현. 재시작하면 상태가 초기화되므로, 프로세스 재시작을 넘나드는
 * 멱등성이 필요하면 Redis 등으로 IdempotencyStore를 직접 구현해서 넘겨야 한다.
 */
export class InMemoryIdempotencyStore implements IdempotencyStore {
  private readonly processed = new Map<string, number>();
  private readonly ttlMs?: number;
  private readonly sweepTimer?: ReturnType<typeof setInterval>;

  constructor(options: InMemoryIdempotencyStoreOptions = {}) {
    this.ttlMs = options.ttlMs;

    if (this.ttlMs) {
      this.sweepTimer = setInterval(() => this.sweep(), options.sweepIntervalMs ?? this.ttlMs);
      this.sweepTimer.unref?.();
    }
  }

  async wasProcessed(key: string): Promise<boolean> {
    const expiresAt = this.processed.get(key);
    if (expiresAt === undefined) return false;
    if (expiresAt < Date.now()) {
      this.processed.delete(key);
      return false;
    }
    return true;
  }

  async markProcessed(key: string): Promise<void> {
    const expiresAt = this.ttlMs ? Date.now() + this.ttlMs : Number.POSITIVE_INFINITY;
    this.processed.set(key, expiresAt);
  }

  /** 타이머를 멈춘다 (테스트, 짧게 쓰고 버리는 인스턴스 정리용). */
  stop(): void {
    if (this.sweepTimer) clearInterval(this.sweepTimer);
  }

  private sweep(): void {
    const now = Date.now();
    for (const [key, expiresAt] of this.processed) {
      if (expiresAt < now) this.processed.delete(key);
    }
  }
}
