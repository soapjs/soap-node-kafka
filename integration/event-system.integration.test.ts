import { KafkaEventBus } from '../src/kafka-event-bus';
import { EventProcessor, EventDispatcher, ExternalEvent } from '@soapjs/soap';
import { setupIntegrationTests, teardownIntegrationTests } from './setup';

describe('Complete Event System Integration Tests', () => {
  let brokers: string[];
  let eventBus: KafkaEventBus<string, Record<string, unknown>>;
  let processor: EventProcessor<string, Record<string, unknown>>;
  let dispatcher: EventDispatcher;

  beforeAll(async () => {
    brokers = await setupIntegrationTests();
    
    eventBus = new KafkaEventBus<string, Record<string, unknown>>({
      brokers,
      topicName: 'test-events',
      groupId: 'test-group'
    });
    
    processor = new EventProcessor(eventBus, {
      retries: 2,
      maxParallelism: 2,
      callbacks: {
        onSuccess: jest.fn(),
        onError: jest.fn()
      }
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
    if (processor) {
      await processor.shutdown();
    }
    if (eventBus) {
      await eventBus.disconnect();
    }
    await teardownIntegrationTests();
  }, 30000);

  describe('End-to-End Event Flow', () => {
    let testProcessor: EventProcessor;

    beforeEach(async () => {
      await eventBus.connect();
      // Create new processor instance for each test
      testProcessor = new EventProcessor(eventBus, {
        retries: 2,
        maxParallelism: 2,
        callbacks: {
          onSuccess: jest.fn(),
          onError: jest.fn()
        }
      });
    });

    afterEach(async () => {
      if (testProcessor) {
        await testProcessor.shutdown();
      }
      // Don't disconnect eventBus here as it's shared across tests
    });

    it('should handle complete event flow: dispatch -> process', async () => {
      const processedEvents: any[] = [];
      
      // Set up processor
      testProcessor.addHandler('system.test', async (payload: any) => {
        processedEvents.push(payload);
      });
      await testProcessor.start();

      // Wait for processor to start
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Dispatch events
      const externalEvent1: ExternalEvent = {
        id: 'system-event-1',
        type: 'system.test',
        data: { message: 'System test message 1' },
        correlationId: 'system-1',
        source: 'test-service',
        timestamp: new Date()
      };

      const externalEvent2: ExternalEvent = {
        id: 'system-event-2',
        type: 'system.test',
        data: { message: 'System test message 2' },
        correlationId: 'system-2',
        source: 'test-service',
        timestamp: new Date()
      };

      await dispatcher.dispatch(externalEvent1);
      await dispatcher.dispatch(externalEvent2);

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 2000));

      expect(processedEvents).toHaveLength(2);
    });



    it('should handle multiple event types with different processors', async () => {
      const userEvents: any[] = [];
      const orderEvents: any[] = [];
      
      // Set up single processor for multiple event types
      const multiProcessor = new EventProcessor(eventBus, {
        retries: 2,
        maxParallelism: 2,
        callbacks: {
          onSuccess: jest.fn(),
          onError: jest.fn()
        }
      });

      multiProcessor.addHandler('user.events', async (payload: any) => {
        userEvents.push(payload);
      });

      multiProcessor.addHandler('order.events', async (payload: any) => {
        orderEvents.push(payload);
      });

      await multiProcessor.start();

      // Wait for processors to start
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Dispatch different event types
      const userEvent1: ExternalEvent = {
        id: 'user-event-1',
        type: 'user.events',
        data: { message: 'User created' },
        correlationId: 'user-1',
        source: 'user-service',
        timestamp: new Date()
      };

      const orderEvent: ExternalEvent = {
        id: 'order-event-1',
        type: 'order.events',
        data: { message: 'Order placed' },
        correlationId: 'order-1',
        source: 'order-service',
        timestamp: new Date()
      };

      const userEvent2: ExternalEvent = {
        id: 'user-event-2',
        type: 'user.events',
        data: { message: 'User updated' },
        correlationId: 'user-2',
        source: 'user-service',
        timestamp: new Date()
      };

      await dispatcher.dispatch(userEvent1);
      await dispatcher.dispatch(orderEvent);
      await dispatcher.dispatch(userEvent2);

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 2000));

      expect(userEvents).toHaveLength(2);
      expect(orderEvents).toHaveLength(1);

      // Cleanup
      await multiProcessor.shutdown();
    });

    it('should handle error scenarios with retries', async () => {
      const onErrorCallback = jest.fn();
      const onRetryCallback = jest.fn();
      
      const customProcessor = new EventProcessor(eventBus, {
        retries: 2,
        maxParallelism: 1,
        callbacks: {
          onError: onErrorCallback
        }
      });

      const customDispatcher = new EventDispatcher(eventBus, {
        maxRetries: 2,
        retryDelay: 100,
        exponentialBackoff: true,
        callbacks: {
          onError: jest.fn(),
          onRetry: onRetryCallback
        }
      });

      // Disconnect eventBus to simulate connection error (like in working test)
      await eventBus.disconnect();

      // Dispatch event that will fail due to connection error
      const errorExternalEvent: ExternalEvent = {
        id: 'error-event',
        type: 'system.error.test',
        data: { message: 'Error test message' },
        correlationId: 'error-test',
        source: 'test-service',
        timestamp: new Date()
      };

      try {
        await customDispatcher.dispatch(errorExternalEvent);
      } catch (error) {
        // Expected to fail
      }

      // Wait for retry attempts
      await new Promise(resolve => setTimeout(resolve, 1000));

      expect(onRetryCallback).toHaveBeenCalled();

      await customProcessor.shutdown();
    });

    it('should handle high throughput scenarios', async () => {
      const processedEvents: string[] = [];
      
      // Set up processor with higher parallelism
      const highThroughputProcessor = new EventProcessor(eventBus, {
        retries: 1,
        maxParallelism: 5,
        callbacks: {
          onSuccess: jest.fn(),
          onError: jest.fn()
        }
      });

      highThroughputProcessor.addHandler('system.throughput.test', async (payload: any) => {
        processedEvents.push(payload);
        // Simulate some processing time
        await new Promise(resolve => setTimeout(resolve, 100));
      });
      await highThroughputProcessor.start();

      // Wait for processor to start
      await new Promise(resolve => setTimeout(resolve, 1000));

      const startTime = Date.now();

      // Dispatch many events
      const eventPromises = [];
      for (let i = 0; i < 10; i++) {
        const externalEvent: ExternalEvent = {
          id: `throughput-event-${i}`,
          type: 'system.throughput.test',
          data: { message: `Throughput message ${i}` },
          correlationId: `throughput-${i}`,
          source: 'test-service',
          timestamp: new Date()
        };
        eventPromises.push(dispatcher.dispatch(externalEvent));
      }

      await Promise.all(eventPromises);

      // Wait for all processing to complete
      await new Promise(resolve => setTimeout(resolve, 2000));

      const endTime = Date.now();
      const totalTime = endTime - startTime;

      expect(processedEvents).toHaveLength(10);
      expect(totalTime).toBeLessThan(3000); // Should process in parallel

      await highThroughputProcessor.shutdown();
    });
  });
});
