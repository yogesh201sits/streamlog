export interface IdempotencyStore {
  hasProcessed(eventId: string): Promise<boolean>;

  markProcessed(eventId: string): Promise<void>;

  remove(eventId: string): Promise<void>;
}

export class MemoryIdempotencyStore
  implements IdempotencyStore
{
  private readonly processed = new Set<string>();

  async hasProcessed(
    eventId: string,
  ): Promise<boolean> {
    return this.processed.has(eventId);
  }

  async markProcessed(
    eventId: string,
  ): Promise<void> {
    this.processed.add(eventId);
  }

  async remove(
    eventId: string,
  ): Promise<void> {
    this.processed.delete(eventId);
  }
}