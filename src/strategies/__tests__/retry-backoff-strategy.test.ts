import { HandlerExecutionError } from '@soapjs/soap';
import { EventBase } from '@soapjs/soap';
import { RetryWithBackoffStrategy } from '../retry-backoff-strategy';

describe('RetryWithBackoffStrategy', () => {
  let strategy: RetryWithBackoffStrategy<string, Record<string, unknown>>;

  beforeEach(() => {
    strategy = new RetryWithBackoffStrategy({
      maxRetries: 3,
      baseDelay: 1000,
      retryTopic: 'test-retry',
      deadLetterTopic: 'test-dlq'
    });
  });

  describe('constructor', () => {
    it('should initialize with default options', () => {
      const defaultStrategy = new RetryWithBackoffStrategy();
      expect(defaultStrategy).toBeDefined();
    });

    it('should initialize with custom options', () => {
      const options = {
        maxRetries: 5,
        baseDelay: 2000,
        retryTopic: 'custom-retry',
        deadLetterTopic: 'custom-dlq'
      };
      const customStrategy = new RetryWithBackoffStrategy(options);
      expect(customStrategy).toBeDefined();
    });
  });

  describe('process', () => {
    it('should process message successfully', async () => {
      const handler = jest.fn().mockResolvedValue(undefined);
      const message: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { correlationId: 'test-123' }
      };

      await strategy.process(message, handler);

      expect(handler).toHaveBeenCalledWith(message);
    });

    it('should retry on first failure', async () => {
      const handler = jest.fn().mockRejectedValue(new Error('Handler failed'));
      const message: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { correlationId: 'test-123' }
      };

      await expect(strategy.process(message, handler)).rejects.toThrow(HandlerExecutionError);
      expect(handler).toHaveBeenCalledWith(message);
    });

    it('should calculate correct backoff delay', () => {
      // Access private method through any type
      const strategy = new RetryWithBackoffStrategy({ baseDelay: 1000 });
      const calculateDelay = (strategy as any).calculateBackoffDelay.bind(strategy);

      expect(calculateDelay(0)).toBe(1000); // 1000 * 2^0 = 1000
      expect(calculateDelay(1)).toBe(2000); // 1000 * 2^1 = 2000
      expect(calculateDelay(2)).toBe(4000); // 1000 * 2^2 = 4000
      expect(calculateDelay(3)).toBe(8000); // 1000 * 2^3 = 8000
    });

    it('should get retry count from message headers', () => {
      const strategy = new RetryWithBackoffStrategy();
      const getRetryCount = (strategy as any).getRetryCount.bind(strategy);

      const messageWithoutRetryCount: EventBase<string, Record<string, unknown>> = {
        message: 'test',
        headers: {}
      };
      expect(getRetryCount(messageWithoutRetryCount)).toBe(0);

      const messageWithRetryCount: EventBase<string, Record<string, unknown>> = {
        message: 'test',
        headers: { 'x-retry-count': 2 }
      };
      expect(getRetryCount(messageWithRetryCount)).toBe(2);
    });

    it('should throw HandlerExecutionError when max retries exceeded', async () => {
      const handler = jest.fn().mockRejectedValue(new Error('Handler failed'));
      const message: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { 'x-retry-count': 3 } // Already at max retries
      };

      await expect(strategy.process(message, handler)).rejects.toThrow(HandlerExecutionError);
    });
  });
});
