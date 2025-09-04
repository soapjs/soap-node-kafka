import { EventValidationError } from '@soapjs/soap';
import { EventBase } from '@soapjs/soap';
import { KafkaProcessingStrategy } from '../base-strategy';

// Concrete implementation for testing
class TestStrategy extends KafkaProcessingStrategy<string, Record<string, unknown>> {
  async process(
    message: EventBase<string, Record<string, unknown>>,
    handler: (event: EventBase<string, Record<string, unknown>>) => Promise<void>
  ): Promise<void> {
    this.validateMessage(message);
    await handler(message);
  }
}

describe('KafkaProcessingStrategy', () => {
  let strategy: TestStrategy;

  beforeEach(() => {
    strategy = new TestStrategy({
      deadLetterTopic: 'test-dlq',
      retryTopic: 'test-retry'
    });
  });

  describe('constructor', () => {
    it('should initialize with default options', () => {
      const defaultStrategy = new TestStrategy();
      expect(defaultStrategy).toBeDefined();
    });

    it('should initialize with custom options', () => {
      const options = {
        deadLetterTopic: 'custom-dlq',
        retryTopic: 'custom-retry',
        maxRetries: 5,
        retryDelay: 2000
      };
      const customStrategy = new TestStrategy(options);
      expect(customStrategy).toBeDefined();
    });
  });

  describe('validateMessage', () => {
    it('should throw EventValidationError for null message', async () => {
      const handler = jest.fn();
      
      await expect(strategy.process(null as any, handler)).rejects.toThrow(EventValidationError);
      expect(handler).not.toHaveBeenCalled();
    });

    it('should throw EventValidationError for undefined message', async () => {
      const handler = jest.fn();
      
      await expect(strategy.process(undefined as any, handler)).rejects.toThrow(EventValidationError);
      expect(handler).not.toHaveBeenCalled();
    });

    it('should throw EventValidationError for message without payload', async () => {
      const handler = jest.fn();
      const message: EventBase<string, Record<string, unknown>> = {
        message: null as any,
        headers: {}
      };
      
      await expect(strategy.process(message, handler)).rejects.toThrow(EventValidationError);
      expect(handler).not.toHaveBeenCalled();
    });

    it('should pass validation for valid message', async () => {
      const handler = jest.fn().mockResolvedValue(undefined);
      const message: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { correlationId: 'test-123' }
      };
      
      await strategy.process(message, handler);
      expect(handler).toHaveBeenCalledWith(message);
    });
  });
});
