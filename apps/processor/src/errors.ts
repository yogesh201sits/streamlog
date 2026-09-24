export class PermanentProcessingError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PermanentProcessingError";
  }
}

export class RetryExhaustedError extends Error {
  constructor(
    message: string,
    public readonly cause?: unknown,
  ) {
    super(message);
    this.name = "RetryExhaustedError";
  }
}