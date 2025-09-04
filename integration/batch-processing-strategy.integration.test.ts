import { setupIntegrationTests, teardownIntegrationTests } from './setup';
import { IntegrationTestHelper } from './test-helpers';
import { BatchProcessingStrategy } from '../src/strategies';
import { EventProcessor, EventDispatcher, EventBase, ExternalEvent } from '@soapjs/soap';

describe('BatchProcessingStrategy Integration Tests', () => {
  let brokers: string[];
  let system: any;

  beforeAll(async () => {
    brokers = await setupIntegrationTests();
  });

  afterAll(async () => {
    await teardownIntegrationTests();
  });

  beforeEach(async () => {
    system = await IntegrationTestHelper.createTestEventSystem(brokers, {
      topicName: 'test-events',
      groupId: 'test-group'
    });
    await IntegrationTestHelper.waitForConnection(system.eventBus);
  });

  afterEach(async () => {
    await IntegrationTestHelper.cleanupEventSystem(system);
  });

  it('should process messages in batch context', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const batchStrategy = new BatchProcessingStrategy<string, Record<string, unknown>>({
      batchSize: 5,
      batchTimeout: 2000,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: batchStrategy,
      callbacks: {
        onSuccess: (event) => {
          console.log('Event processed successfully:', event);
        },
        onError: (error, event) => {
          console.error('Event processing failed:', error, event);
        }
      }
    } as any);

    processor.addHandler('test.batch', async (payload: any) => {
      receivedEvents.push(payload);
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Wait for consumer to be ready - EventProcessor.start() should trigger subscribe()
    // which starts the consumer, but we need to wait for it to be fully ready
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Dispatch multiple messages
    const messages = ['batch1', 'batch2', 'batch3', 'batch4', 'batch5'];
    for (const message of messages) {
      await dispatcher.dispatch({
        id: `batch-${message}`,
        type: 'test.batch',
        data: { message },
        correlationId: `corr-${message}`,
        source: 'test',
        timestamp: new Date()
      });
    }

    // Wait for batch processing
    await new Promise(resolve => setTimeout(resolve, 8000));

    // Debug: log received events
    console.log(`Received ${receivedEvents.length} events:`, receivedEvents.map(e => e.message));

    // All messages should be processed
    expect(receivedEvents).toHaveLength(5);
    expect(receivedEvents.map(e => e.message)).toEqual(expect.arrayContaining(messages.map(m => ({ message: m }))));

    await processor.shutdown();
  }, 20000);

  it('should handle batch processing failures', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const batchStrategy = new BatchProcessingStrategy<string, Record<string, unknown>>({
      batchSize: 3,
      batchTimeout: 1000,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: batchStrategy
    } as any);

    processor.addHandler('test.batch.fail', async (payload: any) => {
      receivedEvents.push(payload);
      
      if ((payload.message && payload.message.includes('fail'))) {
        throw new Error('Batch processing failure');
      }
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch mix of successful and failing messages
    const messages = ['success1', 'fail1', 'success2'];
    for (const message of messages) {
      await dispatcher.dispatch({
        id: `batch-${message}`,
        type: 'test.batch.fail',
        data: { message },
        correlationId: `corr-${message}`,
        source: 'test',
        timestamp: new Date()
      });
    }

    // Wait for batch processing
    await new Promise(resolve => setTimeout(resolve, 2000));

    // All messages should be received (but some will fail)
    expect(receivedEvents).toHaveLength(3);

    await processor.shutdown();
  }, 10000);

  it('should work with custom batch size and timeout', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const batchStrategy = new BatchProcessingStrategy<string, Record<string, unknown>>({
      batchSize: 2,
      batchTimeout: 500,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: batchStrategy
    } as any);

    processor.addHandler('test.batch.custom', async (payload: any) => {
      receivedEvents.push(payload);
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch messages
    const messages = ['custom1', 'custom2'];
    for (const message of messages) {
      await dispatcher.dispatch({
        id: `custom-${message}`,
        type: 'test.batch.custom',
        data: { message },
        correlationId: `corr-${message}`,
        source: 'test',
        timestamp: new Date()
      });
    }

    // Wait for batch processing
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Messages should be processed
    expect(receivedEvents).toHaveLength(2);

    await processor.shutdown();
  }, 10000);

  it('should handle large batch sizes', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const batchStrategy = new BatchProcessingStrategy<string, Record<string, unknown>>({
      batchSize: 10,
      batchTimeout: 1000,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 2,
      strategy: batchStrategy
    } as any);

    processor.addHandler('test.batch.large', async (payload: any) => {
      receivedEvents.push(payload);
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch many messages
    const messages = Array.from({ length: 15 }, (_, i) => `large${i + 1}`);
    for (const message of messages) {
      await dispatcher.dispatch({
        id: `large-${message}`,
        type: 'test.batch.large',
        data: { message },
        correlationId: `corr-${message}`,
        source: 'test',
        timestamp: new Date()
      });
    }

    // Wait for batch processing
    await new Promise(resolve => setTimeout(resolve, 3000));

    // All messages should be processed
    expect(receivedEvents).toHaveLength(15);

    await processor.shutdown();
  }, 15000);
});
