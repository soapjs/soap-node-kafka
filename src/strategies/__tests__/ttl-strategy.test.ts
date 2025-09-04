import { HandlerExecutionError } from '@soapjs/soap';
import { EventBase } from '@soapjs/soap';
import { TTLStrategy } from '../ttl-strategy';

describe('TTLStrategy', () => {
  let strategy: TTLStrategy<string, Record<string, unknown>>;

  beforeEach(() => {
    strategy = new TTLStrategy({
      defaultTTL: 5000,
      deadLetterTopic: 'test-dlq'
    });
  });

  describe('constructor', () => {
    it('should initialize with default options', () => {
      const defaultStrategy = new TTLStrategy();
      expect(defaultStrategy).toBeDefined();
    });

    it('should initialize with custom options', () => {
      const options = {
        defaultTTL: 10000,
        deadLetterExchange: 'custom.dlq'
      };
      const customStrategy = new TTLStrategy(options);
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

    it('should get TTL from message headers', () => {
      const getTTL = (strategy as any).getTTL.bind(strategy);

      const messageWithoutTTL: EventBase<string, Record<string, unknown>> = {
        message: 'test',
        headers: {}
      };
      expect(getTTL(messageWithoutTTL)).toBe(5000); // default TTL

      const messageWithTTL: EventBase<string, Record<string, unknown>> = {
        message: 'test',
        headers: { 'x-ttl': 10000 }
      };
      expect(getTTL(messageWithTTL)).toBe(10000);
    });

    it('should calculate message age correctly', () => {
      const getMessageAge = (strategy as any).getMessageAge.bind(strategy);

      const now = new Date();
      const messageWithTimestamp: EventBase<string, Record<string, unknown>> = {
        message: 'test',
        headers: { 'x-timestamp': now.toISOString() }
      };
      
      // Should be close to 0 (within 100ms tolerance)
      const age = getMessageAge(messageWithTimestamp);
      expect(age).toBeLessThan(100);
    });

    it('should return 0 age for message without timestamp', () => {
      const getMessageAge = (strategy as any).getMessageAge.bind(strategy);

      const messageWithoutTimestamp: EventBase<string, Record<string, unknown>> = {
        message: 'test',
        headers: {}
      };
      
      expect(getMessageAge(messageWithoutTimestamp)).toBe(0);
    });

    it('should throw HandlerExecutionError for expired message', async () => {
      const handler = jest.fn().mockResolvedValue(undefined);
      const oldTimestamp = new Date(Date.now() - 10000); // 10 seconds ago
      const message: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { 
          'x-timestamp': oldTimestamp.toISOString(),
          'x-ttl': 5000 // 5 seconds TTL
        }
      };

      await expect(strategy.process(message, handler)).rejects.toThrow(HandlerExecutionError);
      expect(handler).not.toHaveBeenCalled();
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
  });
});
