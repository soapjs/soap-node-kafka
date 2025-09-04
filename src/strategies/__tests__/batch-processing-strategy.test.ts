import { HandlerExecutionError } from '@soapjs/soap';
import { EventBase } from '@soapjs/soap';
import { BatchProcessingStrategy } from '../batch-processing-strategy';

describe('BatchProcessingStrategy', () => {
  let strategy: BatchProcessingStrategy<string, Record<string, unknown>>;

  beforeEach(() => {
    strategy = new BatchProcessingStrategy({
      batchSize: 5,
      batchTimeout: 2000,
      deadLetterTopic: 'test-dlq'
    });
  });

  describe('constructor', () => {
    it('should initialize with default options', () => {
      const defaultStrategy = new BatchProcessingStrategy();
      expect(defaultStrategy).toBeDefined();
    });

    it('should initialize with custom options', () => {
      const options = {
        batchSize: 10,
        batchTimeout: 5000,
        deadLetterExchange: 'custom.dlq'
      };
      const customStrategy = new BatchProcessingStrategy(options);
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

    it('should throw HandlerExecutionError when handler fails', async () => {
      const handler = jest.fn().mockRejectedValue(new Error('Handler failed'));
      const message: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { correlationId: 'test-123' }
      };

      await expect(strategy.process(message, handler)).rejects.toThrow(HandlerExecutionError);
      expect(handler).toHaveBeenCalledWith(message);
    });

    it('should throw HandlerExecutionError with correct error message', async () => {
      const errorMessage = 'Batch processing failed';
      const handler = jest.fn().mockRejectedValue(new Error(errorMessage));
      const message: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { correlationId: 'test-123' }
      };

      await expect(strategy.process(message, handler)).rejects.toThrow(
        `Batch processing failed: ${errorMessage}`
      );
    });

    it('should handle non-Error exceptions', async () => {
      const handler = jest.fn().mockRejectedValue('String error');
      const message: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { correlationId: 'test-123' }
      };

      await expect(strategy.process(message, handler)).rejects.toThrow(HandlerExecutionError);
    });
  });
});
