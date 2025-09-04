import { setupIntegrationTests, teardownIntegrationTests } from './setup';
import { IntegrationTestHelper } from './test-helpers';
import { PriorityQueueStrategy } from '../src/strategies';
import { EventProcessor, EventDispatcher, EventBase, ExternalEvent } from '@soapjs/soap';

describe('PriorityQueueStrategy Integration Tests', () => {
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

  it('should process messages with different priority levels', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const priorityStrategy = new PriorityQueueStrategy<string, Record<string, unknown>>({
      maxPriority: 10,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: priorityStrategy
    } as any);

    processor.addHandler('test.priority', async (payload: any) => {
      receivedEvents.push(payload);
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch messages with different priorities
    const messages = [
      { message: 'low-priority', priority: 1 },
      { message: 'high-priority', priority: 9 },
      { message: 'medium-priority', priority: 5 }
    ];

    for (const { message, priority } of messages) {
      await dispatcher.dispatch({
        id: `priority-${message}`,
        type: 'test.priority',
        data: { message, priority },
        correlationId: `corr-${message}`,
        source: 'test',
        timestamp: new Date(),
      });
    }

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 2000));

    // All messages should be processed
    expect(receivedEvents).toHaveLength(3);
    expect(receivedEvents.map(e => e.message)).toEqual(
      expect.arrayContaining(messages.map(m => ({ message: m.message, priority: m.priority })))
    );

    await processor.shutdown();
  }, 10000);

  it('should handle messages without priority (default to 0)', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const priorityStrategy = new PriorityQueueStrategy<string, Record<string, unknown>>({
      maxPriority: 10,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: priorityStrategy
    } as any);

    processor.addHandler('test.priority.default', async (payload: any) => {
      receivedEvents.push(payload);
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch message without priority header
    await dispatcher.dispatch({
      id: 'no-priority-message',
      type: 'test.priority.default',
      data: { message: 'no priority message' },
      correlationId: 'corr-no-priority',
      source: 'test',
      timestamp: new Date()
    });

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Message should be processed with default priority (0)
    expect(receivedEvents).toHaveLength(1);
    expect(receivedEvents[0].message).toStrictEqual({ message: 'no priority message' });

    await processor.shutdown();
  }, 10000);

  it('should handle priority processing failures', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const priorityStrategy = new PriorityQueueStrategy<string, Record<string, unknown>>({
      maxPriority: 5,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: priorityStrategy
    } as any);

    processor.addHandler('test.priority.fail', async (payload: any) => {
      receivedEvents.push(payload);
      
      if ((payload.message && payload.message.includes('fail'))) {
        throw new Error('Priority processing failure');
      }
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch messages with different priorities
    const messages = [
      { message: 'success', priority: 3 },
      { message: 'fail', priority: 1 }
    ];

    for (const { message, priority } of messages) {
      await dispatcher.dispatch({
        id: `priority-${message}`,
        type: 'test.priority.fail',
        data: { message, priority },
        correlationId: `corr-${message}`,
        source: 'test',
        timestamp: new Date(),
      });
    }

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Both messages should be received (but one will fail)
    expect(receivedEvents).toHaveLength(2);

    await processor.shutdown();
  }, 10000);

  it('should work with custom max priority', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const priorityStrategy = new PriorityQueueStrategy<string, Record<string, unknown>>({
      maxPriority: 3,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: priorityStrategy
    } as any);

    processor.addHandler('test.priority.custom', async (payload: any) => {
      receivedEvents.push(payload);
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch message with priority within custom range
    await dispatcher.dispatch({
      id: 'custom-priority-message',
      type: 'test.priority.custom',
      data: { message: 'custom priority message' },
      correlationId: 'corr-custom-priority',
      source: 'test',
      timestamp: new Date(),
    });

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Message should be processed
    expect(receivedEvents).toHaveLength(1);
    expect(receivedEvents[0].message).toStrictEqual({ message: 'custom priority message' });

    await processor.shutdown();
  }, 10000);

  it('should extract priority from message headers correctly', () => {
    const priorityStrategy = new PriorityQueueStrategy<string, Record<string, unknown>>({
      maxPriority: 10,
      deadLetterTopic: 'test-dlq'
    });

    const getPriority = (priorityStrategy as any).getPriority.bind(priorityStrategy);

    // Test with priority header
    const messageWithPriority: EventBase<string, Record<string, unknown>> = {
      message: 'test',
      headers: { 'x-priority': 7 }
    };
    expect(getPriority(messageWithPriority)).toBe(7);

    // Test without priority header
    const messageWithoutPriority: EventBase<string, Record<string, unknown>> = {
      message: 'test',
      headers: {}
    };
    expect(getPriority(messageWithoutPriority)).toBe(0);
  });
});
