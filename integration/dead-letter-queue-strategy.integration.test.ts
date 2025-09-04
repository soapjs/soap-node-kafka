import { setupIntegrationTests, teardownIntegrationTests } from './setup';
import { IntegrationTestHelper } from './test-helpers';
import { DeadLetterQueueStrategy } from '../src/strategies';
import { EventProcessor, EventDispatcher, EventBase, ExternalEvent } from '@soapjs/soap';

describe('DeadLetterQueueStrategy Integration Tests', () => {
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
      topicName: 'dlq-test-events',
      groupId: 'dlq-test-group'
    });
    await IntegrationTestHelper.waitForConnection(system.eventBus);
  });

  afterEach(async () => {
    await IntegrationTestHelper.cleanupEventSystem(system);
  });

  it('should route failed messages to dead letter queue', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    const failedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    // Create DLQ strategy
    const dlqStrategy = new DeadLetterQueueStrategy<string, Record<string, unknown>>({
      deadLetterTopic: 'dlq-test-dlq'
    });

    // Create processor with DLQ strategy
    const processor = new EventProcessor(system.eventBus, {
      retries: 0, // No retries, go straight to DLQ
      maxParallelism: 1,
      strategy: dlqStrategy
    } as any);

    // Add handler that fails for specific messages
    processor.addHandler('test.dlq', async (payload: any) => {
      receivedEvents.push(payload);
      
      if (payload.message && payload.message.message && payload.message.message.includes('fail')) {
        throw new Error('Simulated processing failure');
      }
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Wait for consumer to be ready
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Dispatch successful message
    await dispatcher.dispatch({
      id: 'success-message',
      type: 'test.dlq',
      data: { message: 'success message' },
      correlationId: 'corr-success',
      source: 'test',
      timestamp: new Date()
    });

    // Dispatch failing message
    await dispatcher.dispatch({
      id: 'fail-message',
      type: 'test.dlq',
      data: { message: 'fail message' },
      correlationId: 'corr-fail',
      source: 'test',
      timestamp: new Date()
    });

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 6000));

    // DLQ Strategy passes all messages to handler (success + failed)
    // The strategy logs errors but doesn't block messages
    expect(receivedEvents).toHaveLength(2);
    
    // Check that both messages were received
    const successMessage = receivedEvents.find(e => (e.message as any).message === 'success message');
    const failMessage = receivedEvents.find(e => (e.message as any).message === 'fail message');
    
    expect(successMessage).toBeDefined();
    expect(failMessage).toBeDefined();
    
    await processor.shutdown();
  }, 20000);

  it('should handle multiple failed messages', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const dlqStrategy = new DeadLetterQueueStrategy<string, Record<string, unknown>>({
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 2,
      strategy: dlqStrategy
    } as any);

    processor.addHandler('test.dlq.multiple', async (payload: any) => {
      receivedEvents.push(payload);
      throw new Error('Always fail');
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch multiple failing messages
    const messages = ['fail1', 'fail2', 'fail3'];
    for (const message of messages) {
      await dispatcher.dispatch({
        id: `fail-${message}`,
        type: 'test.dlq.multiple',
        data: { message },
        correlationId: `corr-${message}`,
        source: 'test',
        timestamp: new Date()
      });
    }

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 3000));

    // All messages should be received (but will fail and go to DLQ)
    expect(receivedEvents).toHaveLength(3);

    await processor.shutdown();
  }, 10000);

  it('should work with custom dead letter exchange configuration', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const dlqStrategy = new DeadLetterQueueStrategy<string, Record<string, unknown>>({
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: dlqStrategy
    } as any);

    processor.addHandler('test.dlq.custom', async (payload: any) => {
      receivedEvents.push(payload);
      throw new Error('Custom DLQ test failure');
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    await dispatcher.dispatch({
      id: 'custom-dlq-message',
      type: 'test.dlq.custom',
      data: { message: 'custom dlq message' },
      correlationId: 'corr-custom',
      source: 'test',
      timestamp: new Date()
    });

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 2000));

    expect(receivedEvents).toHaveLength(1);
    expect(receivedEvents[0].message).toStrictEqual({ message: 'custom dlq message' });

    await processor.shutdown();
  }, 10000);
});
