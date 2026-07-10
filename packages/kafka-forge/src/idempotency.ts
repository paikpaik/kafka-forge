export interface IdempotencyStore {
  wasProcessed(key: string): Promise<boolean>;
  markProcessed(key: string): Promise<void>;
}

export class InMemoryIdempotencyStore implements IdempotencyStore {
  private readonly processedKeys = new Set<string>();

  async wasProcessed(key: string): Promise<boolean> {
    return this.processedKeys.has(key);
  }

  async markProcessed(key: string): Promise<void> {
    this.processedKeys.add(key);
  }
}
