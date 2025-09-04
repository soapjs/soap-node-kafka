import { HandlerExecutionError } from '@soapjs/soap';
import { EventBase } from '@soapjs/soap';
import { PriorityQueueStrategy } from '../priority-queue-strategy';

describe('PriorityQueueStrategy', () => {
  let strategy: PriorityQueueStrategy<string, Record<string, unknown>>;

  beforeEach(() => {
    strategy = new PriorityQueueStrategy({
      maxPriority: 10,
      deadLetterTopic: 'test-dlq'
    });
  });

  describe('constructor', () => {
    it('should initialize with default options', () => {
      const defaultStrategy = new PriorityQueueStrategy();
      expect(defaultStrategy).toBeDefined();
    });

    it('should initialize with custom options', () => {
      const options = {
        maxPriority: 5,
        deadLetterExchange: 'custom.dlq'
      };
      const customStrategy = new PriorityQueueStrategy(options);
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

    it('should get priority from message headers', () => {
      const getPriority = (strategy as any).getPriority.bind(strategy);

      const messageWithoutPriority: EventBase<string, Record<string, unknown>> = {
        message: 'test',
        headers: {}
      };
      expect(getPriority(messageWithoutPriority)).toBe(0);

      const messageWithPriority: EventBase<string, Record<string, unknown>> = {
        message: 'test',
        headers: { 'x-priority': 5 }
      };
      expect(getPriority(messageWithPriority)).toBe(5);
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
      const errorMessage = 'Priority processing failed';
      const handler = jest.fn().mockRejectedValue(new Error(errorMessage));
      const message: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { correlationId: 'test-123' }
      };

      await expect(strategy.process(message, handler)).rejects.toThrow(
        `Priority processing failed: ${errorMessage}`
      );
    });
  });
});
