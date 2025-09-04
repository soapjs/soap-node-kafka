import { KafkaEventBus } from '../src/kafka-event-bus';
import { EventProcessor } from '@soapjs/soap';
import { setupIntegrationTests, teardownIntegrationTests } from './setup';

describe('EventProcessor Integration Tests', () => {
  let brokers: string[];
  let eventBus: KafkaEventBus<string, Record<string, unknown>>;
  let processor: EventProcessor<string, Record<string, unknown>>;

  beforeAll(async () => {
    brokers = await setupIntegrationTests();
    
    eventBus = new KafkaEventBus<string, Record<string, unknown>>({
      brokers,
      topicName: 'test-events',
      groupId: 'test-group'
    });
    
    processor = new EventProcessor(eventBus, {
      retries: 2,
      maxParallelism: 3,
      callbacks: {
        onSuccess: jest.fn(),
        onError: jest.fn()
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

  describe('Event Processing', () => {
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

    it('should process events successfully', async () => {
      const processedEvents: any[] = [];
      
      // Add handler and start processor
      testProcessor.addHandler('processor.test', async (payload: any) => {
        processedEvents.push(payload);
      });
      await testProcessor.start();

      // Wait for processor to start
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Publish events
      await eventBus.publish('processor.test', {
        message: 'Test message 1',
        headers: { correlation_id: 'test-1', timestamp: new Date().toISOString() }
      });

      await eventBus.publish('processor.test', {
        message: 'Test message 2',
        headers: { correlation_id: 'test-2', timestamp: new Date().toISOString() }
      });

      // Wait for processing to complete
      await new Promise(resolve => setTimeout(resolve, 2000));

      expect(processedEvents).toHaveLength(2);
      expect(processedEvents).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            message: 'Test message 1',
            headers: expect.objectContaining({ correlation_id: 'test-1' })
          }),
          expect.objectContaining({
            message: 'Test message 2',
            headers: expect.objectContaining({ correlation_id: 'test-2' })
          })
        ])
      );
    });

    it('should handle processing errors with retries', async () => {
      const onErrorCallback = jest.fn();
      const onSuccessCallback = jest.fn();
      
      const customProcessor = new EventProcessor(eventBus, {
        retries: 2,
        retryDelay: 100,
        maxParallelism: 1,
        callbacks: {
          onError: onErrorCallback,
          onSuccess: onSuccessCallback
        }
      });

      let attemptCount = 0;
      
      // Add handler and start processor
      customProcessor.addHandler('processor-error.test', async (payload: any) => {
        attemptCount++;
        if (attemptCount < 3) {
          throw new Error(`Simulated processing error (attempt ${attemptCount})`);
        }
        // Success on third attempt
      });
      await customProcessor.start();

      // Wait for processor to start
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Publish event that will fail initially but succeed after retries
      await eventBus.publish('processor-error.test', {
        message: 'Error test message',
        headers: { correlation_id: 'error-test', timestamp: new Date().toISOString() }
      });

      // Wait for processing to complete
      await new Promise(resolve => setTimeout(resolve, 3000));

      expect(attemptCount).toBe(3);
      expect(onErrorCallback).toHaveBeenCalled();
      expect(onSuccessCallback).toHaveBeenCalled();

      await customProcessor.shutdown();
    });

    it('should handle max parallelism correctly', async () => {
      const processingTimes: number[] = [];
      const startTime = Date.now();
      
      // Start processor
      testProcessor.addHandler('processor-parallel.test', async (payload: any) => {
        const processingTime = Date.now() - startTime;
        processingTimes.push(processingTime);
        
        // Simulate some processing time
        await new Promise(resolve => setTimeout(resolve, 500));
      });
      await testProcessor.start();

      // Wait for processor to start
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Publish 3 events simultaneously
      await Promise.all([
        eventBus.publish('processor-parallel.test', {
          message: 'Parallel message 1',
          headers: { correlation_id: 'parallel-1', timestamp: new Date().toISOString() }
        }),
        eventBus.publish('processor-parallel.test', {
          message: 'Parallel message 2',
          headers: { correlation_id: 'parallel-2', timestamp: new Date().toISOString() }
        }),
        eventBus.publish('processor-parallel.test', {
          message: 'Parallel message 3',
          headers: { correlation_id: 'parallel-3', timestamp: new Date().toISOString() }
        })
      ]);

      // Wait for processing to complete
      await new Promise(resolve => setTimeout(resolve, 2000));

      expect(processingTimes).toHaveLength(3);
      // With maxParallelism=3, all events should start processing around the same time
      const maxTimeDiff = Math.max(...processingTimes) - Math.min(...processingTimes);
      expect(maxTimeDiff).toBeLessThan(1000); // Should be processed in parallel
    });
  });
});
