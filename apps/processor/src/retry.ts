import {
  PermanentProcessingError,
  RetryExhaustedError,
} from "./errors";

export type RetryOptions = {
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
};

const sleep = async (
  milliseconds: number,
): Promise<void> => {
  await new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
};

export async function withRetry<T>(
  operation: () => Promise<T>,
  options: RetryOptions,
): Promise<T> {
  let lastError: unknown;

  for (
    let attempt = 1;
    attempt <= options.maxAttempts;
    attempt++
  ) {
    try {
      return await operation();
    } catch (error) {
      if (
        error instanceof PermanentProcessingError
      ) {
        throw error;
      }

      lastError = error;

      if (attempt === options.maxAttempts) {
        break;
      }

      const exponentialDelay =
        options.baseDelayMs *
        2 ** (attempt - 1);

      const cappedDelay = Math.min(
        exponentialDelay,
        options.maxDelayMs,
      );

      const jitter = Math.floor(
        Math.random() * cappedDelay * 0.2,
      );

      const delay = cappedDelay + jitter;

      console.log(
        `[Retry] attempt=${attempt} failed, ` +
          `retrying in ${delay}ms`,
      );

      await sleep(delay);
    }
  }

  throw new RetryExhaustedError(
    `Operation failed after ${options.maxAttempts} attempts`,
    lastError,
  );
}
