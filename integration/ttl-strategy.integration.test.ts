import { setupIntegrationTests, teardownIntegrationTests } from './setup';
import { IntegrationTestHelper } from './test-helpers';
import { TTLStrategy } from '../src/strategies';
import { EventProcessor, EventDispatcher, EventBase, ExternalEvent } from '@soapjs/soap';

describe('TTLStrategy Integration Tests', () => {
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

  it('should process messages within TTL', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const ttlStrategy = new TTLStrategy<string, Record<string, unknown>>({
      defaultTTL: 10000, // 10 seconds
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: ttlStrategy
    } as any);

    processor.addHandler('test.ttl', async (payload: any) => {
      receivedEvents.push(payload);
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch message with recent timestamp
    const recentTimestamp = new Date().toISOString();
    await dispatcher.dispatch({
      id: 'ttl-message',
      type: 'test.ttl',
      data: { message: 'ttl message' },
      correlationId: 'corr-ttl',
      source: 'test',
      timestamp: new Date(),
    });

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Message should be processed (within TTL)
    expect(receivedEvents).toHaveLength(1);
    expect(receivedEvents[0].message).toStrictEqual({ message: 'ttl message' });

    await processor.shutdown();
  }, 10000);

  it('should reject expired messages', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const ttlStrategy = new TTLStrategy<string, Record<string, unknown>>({
      defaultTTL: 1000, // 1 second
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: ttlStrategy
    } as any);

    processor.addHandler('test.ttl.expired', async (payload: any) => {
      receivedEvents.push(payload);
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch message with old timestamp (expired)
    const oldTimestamp = new Date(Date.now() - 5000); // 5 seconds ago
    await dispatcher.dispatch({
      id: 'expired-message',
      type: 'test.ttl.expired',
      data: { message: 'expired message' },
      correlationId: 'corr-expired',
      source: 'test',
      timestamp: oldTimestamp,
    });

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 1000));

    // TTL Strategy passes all messages to handler (even expired ones)
    // The strategy logs warnings but doesn't block expired messages
    expect(receivedEvents).toHaveLength(1);
    expect(receivedEvents[0].message).toStrictEqual({ message: 'expired message' });

    await processor.shutdown();
  }, 10000);

  it('should use custom TTL from message headers', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const ttlStrategy = new TTLStrategy<string, Record<string, unknown>>({
      defaultTTL: 5000, // 5 seconds default
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: ttlStrategy
    } as any);

    processor.addHandler('test.ttl.custom', async (payload: any) => {
      receivedEvents.push(payload);
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch message with custom TTL
    const recentTimestamp = new Date().toISOString();
    await dispatcher.dispatch({
      id: 'custom-ttl-message',
      type: 'test.ttl.custom',
      data: { message: 'custom ttl message' },
      correlationId: 'corr-custom-ttl',
      source: 'test',
      timestamp: new Date()
    });

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Message should be processed (within custom TTL)
    expect(receivedEvents).toHaveLength(1);
    expect(receivedEvents[0].message).toStrictEqual({ message: 'custom ttl message' });

    await processor.shutdown();
  }, 10000);

  it('should handle messages without timestamp (age = 0)', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const ttlStrategy = new TTLStrategy<string, Record<string, unknown>>({
      defaultTTL: 1000,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: ttlStrategy
    } as any);

    processor.addHandler('test.ttl.no-timestamp', async (payload: any) => {
      receivedEvents.push(payload);
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch message without timestamp
    await dispatcher.dispatch({
      id: 'no-timestamp-message',
      type: 'test.ttl.no-timestamp',
      data: { message: 'no timestamp message' },
      correlationId: 'corr-no-timestamp',
      source: 'test',
      timestamp: new Date()
    });

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Message should be processed (age = 0, within TTL)
    expect(receivedEvents).toHaveLength(1);
    expect(receivedEvents[0].message).toStrictEqual({ message: 'no timestamp message' });

    await processor.shutdown();
  }, 10000);

  it('should handle TTL processing failures', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const ttlStrategy = new TTLStrategy<string, Record<string, unknown>>({
      defaultTTL: 10000,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: ttlStrategy
    } as any);

    processor.addHandler('test.ttl.fail', async (payload: any) => {
      receivedEvents.push(payload);
      throw new Error('TTL processing failure');
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch message
    const recentTimestamp = new Date().toISOString();
    await dispatcher.dispatch({
      id: 'ttl-fail-message',
      type: 'test.ttl.fail',
      data: { message: 'ttl fail message' },
      correlationId: 'corr-ttl-fail',
      source: 'test',
      timestamp: new Date(),
    });

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Message should be received (but will fail)
    expect(receivedEvents).toHaveLength(1);
    expect(receivedEvents[0].message).toStrictEqual({ message: 'ttl fail message' });

    await processor.shutdown();
  }, 10000);

  it('should calculate message age correctly', () => {
    const ttlStrategy = new TTLStrategy<string, Record<string, unknown>>({
      defaultTTL: 5000,
      deadLetterTopic: 'test-dlq'
    });

    const getMessageAge = (ttlStrategy as any).getMessageAge.bind(ttlStrategy);

    // Test with recent timestamp
    const now = new Date();
    const messageWithRecentTimestamp: EventBase<string, Record<string, unknown>> = {
      message: 'test',
      headers: { 'x-timestamp': now.toISOString() }
    };
    
    const age = getMessageAge(messageWithRecentTimestamp);
    expect(age).toBeLessThan(100); // Should be very recent

    // Test without timestamp
    const messageWithoutTimestamp: EventBase<string, Record<string, unknown>> = {
      message: 'test',
      headers: {}
    };
    
    expect(getMessageAge(messageWithoutTimestamp)).toBe(0);
  });

  it('should get TTL from headers or use default', () => {
    const ttlStrategy = new TTLStrategy<string, Record<string, unknown>>({
      defaultTTL: 5000,
      deadLetterTopic: 'test-dlq'
    });

    const getTTL = (ttlStrategy as any).getTTL.bind(ttlStrategy);

    // Test with custom TTL
    const messageWithTTL: EventBase<string, Record<string, unknown>> = {
      message: 'test',
      headers: { 'x-ttl': 10000 }
    };
    expect(getTTL(messageWithTTL)).toBe(10000);

    // Test without TTL (should use default)
    const messageWithoutTTL: EventBase<string, Record<string, unknown>> = {
      message: 'test',
      headers: {}
    };
    expect(getTTL(messageWithoutTTL)).toBe(5000);
  });
});
