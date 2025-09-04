import { KafkaEventBus } from '../src/kafka-event-bus';
import { EventDispatcher, ExternalEvent } from '@soapjs/soap';
import { setupIntegrationTests, teardownIntegrationTests } from './setup';

describe('EventDispatcher Integration Tests', () => {
  let brokers: string[];
  let eventBus: KafkaEventBus<string, Record<string, unknown>>;
  let dispatcher: EventDispatcher;

  beforeAll(async () => {
    brokers = await setupIntegrationTests();
    
    eventBus = new KafkaEventBus<string, Record<string, unknown>>({
      brokers,
      topicName: 'test-events',
      groupId: 'test-group'
    });
    
    dispatcher = new EventDispatcher(eventBus, {
      maxRetries: 3,
      retryDelay: 100,
      exponentialBackoff: true,
      callbacks: {
        onSuccess: jest.fn(),
        onError: jest.fn(),
        onRetry: jest.fn()
      }
    });
  }, 60000);

  afterAll(async () => {
    if (eventBus) {
      await eventBus.disconnect();
    }
    await teardownIntegrationTests();
  }, 30000);

  describe('Event Dispatching', () => {
    beforeEach(async () => {
      await eventBus.connect();
    });

    afterEach(async () => {
      await eventBus.disconnect();
    });

    it('should dispatch events successfully', async () => {
      const receivedEvents: string[] = [];
      
      // Subscribe to events
      await eventBus.subscribe('dispatcher.test', (event) => {
        receivedEvents.push(event.message);
      });

      // Wait for subscription to be ready
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Dispatch events
      const externalEvent1: ExternalEvent = {
        id: 'event-1',
        type: 'dispatcher.test',
        data: { message: 'Test message 1' },
        correlationId: 'test-1',
        source: 'test-service',
        timestamp: new Date()
      };

      const externalEvent2: ExternalEvent = {
        id: 'event-2',
        type: 'dispatcher.test',
        data: { message: 'Test message 2' },
        correlationId: 'test-2',
        source: 'test-service',
        timestamp: new Date()
      };

      await dispatcher.dispatch(externalEvent1);
      await dispatcher.dispatch(externalEvent2);

      // Wait for events to be received
      await new Promise(resolve => setTimeout(resolve, 2000));

      expect(receivedEvents).toHaveLength(2);
      expect(receivedEvents).toContainEqual({ message: 'Test message 1' });
      expect(receivedEvents).toContainEqual({ message: 'Test message 2' });
    });

    it('should handle dispatch errors with retries', async () => {
      const onErrorCallback = jest.fn();
      const onRetryCallback = jest.fn();
      
      const customDispatcher = new EventDispatcher(eventBus, {
        maxRetries: 2,
        retryDelay: 100,
        exponentialBackoff: true,
        callbacks: {
          onError: onErrorCallback,
          onRetry: onRetryCallback
        }
      });

      // Disconnect eventBus to simulate error
      await eventBus.disconnect();

      const externalEvent: ExternalEvent = {
        id: 'error-event',
        type: 'dispatcher-error.test',
        data: { message: 'Error test message' },
        correlationId: 'error-test',
        source: 'test-service',
        timestamp: new Date()
      };

      try {
        await customDispatcher.dispatch(externalEvent);
      } catch (error) {
        // Expected to fail
      }

      expect(onRetryCallback).toHaveBeenCalled();
      expect(onErrorCallback).toHaveBeenCalled();
    });

    it('should use exponential backoff for retries', async () => {
      const onRetryCallback = jest.fn();
      const customDispatcher = new EventDispatcher(eventBus, {
        maxRetries: 3,
        retryDelay: 100,
        exponentialBackoff: true,
        callbacks: {
          onRetry: onRetryCallback
        }
      });

      // Disconnect eventBus to simulate error
      await eventBus.disconnect();

      const externalEvent: ExternalEvent = {
        id: 'backoff-event',
        type: 'dispatcher-backoff.test',
        data: { message: 'Backoff test message' },
        correlationId: 'backoff-test',
        source: 'test-service',
        timestamp: new Date()
      };

      try {
        await customDispatcher.dispatch(externalEvent);
      } catch (error) {
        // Expected to fail
      }

      expect(onRetryCallback).toHaveBeenCalled();
      expect(onRetryCallback).toHaveBeenCalledTimes(2); // 2 retries
    });

    it('should generate correlation IDs automatically', async () => {
      const receivedEvents: any[] = [];
      
      // Subscribe to events
      await eventBus.subscribe('dispatcher-correlation.test', (event) => {
        receivedEvents.push(event);
      });

      // Wait for subscription to be ready
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Dispatch event without correlation_id
      const externalEvent: ExternalEvent = {
        id: 'correlation-event',
        type: 'dispatcher-correlation.test',
        data: { message: 'Correlation test message' },
        correlationId: 'test-correlation-id',
        source: 'test-service',
        timestamp: new Date()
      };

      await dispatcher.dispatch(externalEvent);

      // Wait for event to be received
      await new Promise(resolve => setTimeout(resolve, 1000));

      expect(receivedEvents).toHaveLength(1);
      expect(receivedEvents[0].headers.correlationId).toBeDefined();
      expect(receivedEvents[0].headers.correlationId).toBe('test-correlation-id');
    });

    it('should generate timestamps automatically', async () => {
      const receivedEvents: any[] = [];
      
      // Subscribe to events
      await eventBus.subscribe('dispatcher-timestamp.test', (event) => {
        receivedEvents.push(event);
      });

      // Wait for subscription to be ready
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Dispatch event without timestamp
      const externalEvent: ExternalEvent = {
        id: 'timestamp-event',
        type: 'dispatcher-timestamp.test',
        data: { message: 'Timestamp test message' },
        correlationId: 'timestamp-test',
        source: 'test-service',
        timestamp: new Date()
      };

      await dispatcher.dispatch(externalEvent);

      // Wait for event to be received
      await new Promise(resolve => setTimeout(resolve, 1000));

      expect(receivedEvents).toHaveLength(1);
      expect(receivedEvents[0].headers.timestamp).toBeDefined();
      expect(new Date(receivedEvents[0].headers.timestamp)).toBeInstanceOf(Date);
    });
  });
});
