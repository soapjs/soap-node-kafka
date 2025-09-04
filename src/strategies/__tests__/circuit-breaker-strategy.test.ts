import { HandlerExecutionError } from '@soapjs/soap';
import { EventBase } from '@soapjs/soap';
import { CircuitBreakerStrategy } from '../circuit-breaker-strategy';

describe('CircuitBreakerStrategy', () => {
  let strategy: CircuitBreakerStrategy<string, Record<string, unknown>>;

  beforeEach(() => {
    strategy = new CircuitBreakerStrategy({
      failureThreshold: 3,
      recoveryTimeout: 1000,
      deadLetterTopic: 'test-dlq'
    });
  });

  describe('constructor', () => {
    it('should initialize with default options', () => {
      const defaultStrategy = new CircuitBreakerStrategy();
      expect(defaultStrategy).toBeDefined();
    });

    it('should initialize with custom options', () => {
      const options = {
        failureThreshold: 5,
        recoveryTimeout: 2000,
        deadLetterExchange: 'custom.dlq'
      };
      const customStrategy = new CircuitBreakerStrategy(options);
      expect(customStrategy).toBeDefined();
    });
  });

  describe('process', () => {
    it('should process message successfully when circuit is closed', async () => {
      const handler = jest.fn().mockResolvedValue(undefined);
      const message: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { correlationId: 'test-123' }
      };

      await strategy.process(message, handler);

      expect(handler).toHaveBeenCalledWith(message);
    });

    it('should open circuit after reaching failure threshold', async () => {
      const handler = jest.fn().mockRejectedValue(new Error('Handler failed'));
      const message: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { correlationId: 'test-123' }
      };

      // First 3 failures should be allowed
      for (let i = 0; i < 3; i++) {
        await expect(strategy.process(message, handler)).rejects.toThrow(HandlerExecutionError);
      }

      // 4th failure should open the circuit
      await expect(strategy.process(message, handler)).rejects.toThrow(HandlerExecutionError);
    });

    it('should transition to half-open after recovery timeout', async () => {
      const handler = jest.fn().mockRejectedValue(new Error('Handler failed'));
      const message: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { correlationId: 'test-123' }
      };

      // Open the circuit
      for (let i = 0; i < 3; i++) {
        await expect(strategy.process(message, handler)).rejects.toThrow(HandlerExecutionError);
      }

      // Wait for recovery timeout
      await new Promise(resolve => setTimeout(resolve, 1100));

      // Should transition to half-open
      await expect(strategy.process(message, handler)).rejects.toThrow(HandlerExecutionError);
    });

    it('should reset circuit to closed after successful half-open', async () => {
      const failingHandler = jest.fn().mockRejectedValue(new Error('Handler failed'));
      const successHandler = jest.fn().mockResolvedValue(undefined);
      const message: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { correlationId: 'test-123' }
      };

      // Open the circuit
      for (let i = 0; i < 3; i++) {
        await expect(strategy.process(message, failingHandler)).rejects.toThrow(HandlerExecutionError);
      }

      // Wait for recovery timeout
      await new Promise(resolve => setTimeout(resolve, 1100));

      // Should succeed and reset circuit
      await strategy.process(message, successHandler);
      expect(successHandler).toHaveBeenCalledWith(message);
    });

    it('should throw HandlerExecutionError when circuit is open', async () => {
      const handler = jest.fn().mockRejectedValue(new Error('Handler failed'));
      const message: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { correlationId: 'test-123' }
      };

      // Open the circuit
      for (let i = 0; i < 3; i++) {
        await expect(strategy.process(message, handler)).rejects.toThrow(HandlerExecutionError);
      }

      // Circuit should be open now
      await expect(strategy.process(message, handler)).rejects.toThrow(
        'Circuit breaker is OPEN, message will be routed to dead letter queue'
      );
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
