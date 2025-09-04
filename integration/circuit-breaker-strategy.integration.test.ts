import { setupIntegrationTests, teardownIntegrationTests } from './setup';
import { IntegrationTestHelper } from './test-helpers';
import { CircuitBreakerStrategy } from '../src/strategies';
import { EventProcessor, EventDispatcher, EventBase, ExternalEvent } from '@soapjs/soap';

describe('CircuitBreakerStrategy Integration Tests', () => {
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

  it('should process messages when circuit is closed', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const circuitBreakerStrategy = new CircuitBreakerStrategy<string, Record<string, unknown>>({
      failureThreshold: 3,
      recoveryTimeout: 2000,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: circuitBreakerStrategy
    } as any);

    processor.addHandler('test.circuit.closed', async (payload: any) => {
      receivedEvents.push(payload);
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch successful message
    await dispatcher.dispatch({
      id: 'success-message',
      type: 'test.circuit.closed',
      data: { message: 'success message' },
      correlationId: 'corr-success',
      source: 'test',
      timestamp: new Date()
    });

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Message should be processed
    expect(receivedEvents).toHaveLength(1);
    expect(receivedEvents[0].message).toStrictEqual({ message: 'success message' });

    await processor.shutdown();
  }, 10000);

  it('should open circuit after reaching failure threshold', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const circuitBreakerStrategy = new CircuitBreakerStrategy<string, Record<string, unknown>>({
      failureThreshold: 2,
      recoveryTimeout: 3000,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: circuitBreakerStrategy
    } as any);

    processor.addHandler('test.circuit.open', async (payload: any) => {
      receivedEvents.push(payload);
      throw new Error('Always fail');
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch messages to trigger circuit opening
    for (let i = 0; i < 3; i++) {
      await dispatcher.dispatch({
        id: `fail-message-${i}`,
        type: 'test.circuit.open',
        data: { message: `fail message ${i}` },
        correlationId: `corr-fail-${i}`,
        source: 'test',
        timestamp: new Date()
      });
    }

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 2000));

    // All messages should be received (but will fail)
    expect(receivedEvents).toHaveLength(3);

    await processor.shutdown();
  }, 10000);

  it('should transition to half-open after recovery timeout', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const circuitBreakerStrategy = new CircuitBreakerStrategy<string, Record<string, unknown>>({
      failureThreshold: 2,
      recoveryTimeout: 1000,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: circuitBreakerStrategy
    } as any);

    processor.addHandler('test.circuit.half-open', async (payload: any) => {
      receivedEvents.push(payload);
      throw new Error('Always fail');
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Open the circuit
    for (let i = 0; i < 2; i++) {
      await dispatcher.dispatch({
        id: `open-circuit-${i}`,
        type: 'test.circuit.half-open',
        data: { message: `open circuit ${i}` },
        correlationId: `corr-open-${i}`,
        source: 'test',
        timestamp: new Date()
      });
    }

    // Wait for circuit to open
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Wait for recovery timeout
    await new Promise(resolve => setTimeout(resolve, 1500));

    // Circuit should be half-open now, dispatch another message
    await dispatcher.dispatch({
      id: 'half-open-message',
      type: 'test.circuit.half-open',
      data: { message: 'half-open message' },
      correlationId: 'corr-half-open',
      source: 'test',
      timestamp: new Date()
    });

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Should have received all messages
    expect(receivedEvents).toHaveLength(3);

    await processor.shutdown();
  }, 15000);

  it('should reset circuit to closed after successful half-open', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const circuitBreakerStrategy = new CircuitBreakerStrategy<string, Record<string, unknown>>({
      failureThreshold: 2,
      recoveryTimeout: 1000,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: circuitBreakerStrategy
    } as any);

    let shouldFail = true;

    processor.addHandler('test.circuit.reset', async (payload: any) => {
      receivedEvents.push(payload);
      
      if (shouldFail) {
        throw new Error('Fail initially');
      }
      // Success after circuit opens
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Open the circuit with failures
    for (let i = 0; i < 2; i++) {
      await dispatcher.dispatch({
        id: `fail-to-open-${i}`,
        type: 'test.circuit.reset',
        data: { message: `fail to open ${i}` },
        correlationId: `corr-fail-open-${i}`,
        source: 'test',
        timestamp: new Date()
      });
    }

    // Wait for circuit to open
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Wait for recovery timeout
    await new Promise(resolve => setTimeout(resolve, 1500));

    // Now allow success
    shouldFail = false;

    // Dispatch successful message (should reset circuit)
    await dispatcher.dispatch({
      id: 'reset-message',
      type: 'test.circuit.reset',
      data: { message: 'reset message' },
      correlationId: 'corr-reset',
      source: 'test',
      timestamp: new Date()
    });

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Should have received all messages
    expect(receivedEvents).toHaveLength(3);

    await processor.shutdown();
  }, 15000);

  it('should work with custom failure threshold and recovery timeout', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const circuitBreakerStrategy = new CircuitBreakerStrategy<string, Record<string, unknown>>({
      failureThreshold: 1,
      recoveryTimeout: 500,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 1,
      strategy: circuitBreakerStrategy
    } as any);

    processor.addHandler('test.circuit.custom', async (payload: any) => {
      receivedEvents.push(payload);
      throw new Error('Custom circuit breaker failure');
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Single failure should open circuit (threshold = 1)
    await dispatcher.dispatch({
      id: 'custom-fail-message',
      type: 'test.circuit.custom',
      data: { message: 'custom fail message' },
      correlationId: 'corr-custom-fail',
      source: 'test',
      timestamp: new Date()
    });

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Message should be received
    expect(receivedEvents).toHaveLength(1);

    await processor.shutdown();
  }, 10000);

  it('should handle multiple concurrent failures', async () => {
    const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
    
    const circuitBreakerStrategy = new CircuitBreakerStrategy<string, Record<string, unknown>>({
      failureThreshold: 3,
      recoveryTimeout: 2000,
      deadLetterTopic: 'test-dlq'
    });

    const processor = new EventProcessor(system.eventBus, {
      retries: 0,
      maxParallelism: 3,
      strategy: circuitBreakerStrategy
    } as any);

    processor.addHandler('test.circuit.concurrent', async (payload: any) => {
      receivedEvents.push(payload);
      throw new Error('Concurrent failure');
    });

    await processor.start();
    const dispatcher = new EventDispatcher(system.eventBus as any);

    // Dispatch multiple concurrent failing messages
    const promises = [];
    for (let i = 0; i < 5; i++) {
      promises.push(
        dispatcher.dispatch({
          id: `concurrent-fail-${i}`,
          type: 'test.circuit.concurrent',
          data: { message: `concurrent fail ${i}` },
          correlationId: `corr-concurrent-${i}`,
          source: 'test',
          timestamp: new Date()
        })
      );
    }

    await Promise.all(promises);

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 2000));

    // All messages should be received
    expect(receivedEvents).toHaveLength(5);

    await processor.shutdown();
  }, 10000);
});
