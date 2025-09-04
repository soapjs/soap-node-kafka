import { KafkaEventBus } from '../src/kafka-event-bus';
import { DeadLetterQueueStrategy, RetryWithBackoffStrategy, BatchProcessingStrategy, PriorityQueueStrategy, TTLStrategy, CircuitBreakerStrategy } from '../src/strategies';
import { EventProcessor, EventDispatcher, EventBase } from '@soapjs/soap';

/**
 * Example demonstrating different Kafka processing strategies.
 * 
 * This file shows how to use the various Kafka-specific processing
 * strategies with the EventProcessor.
 */

// Example event types
interface UserEvent {
  userId: string;
  action: 'created' | 'updated' | 'deleted';
  timestamp: Date;
}

interface UserEventHeaders {
  correlationId: string;
  source: string;
  priority?: number;
  ttl?: number;
}

/**
 * Example 1: Dead Letter Queue Strategy
 * 
 * This strategy routes failed messages to a dead letter topic
 * for manual inspection and recovery.
 */
export async function deadLetterQueueExample() {
  console.log('=== Dead Letter Queue Strategy Example ===');
  
  const eventBus = new KafkaEventBus<UserEvent, UserEventHeaders>({
    brokers: ['localhost:9092'],
    topicName: 'user-events',
    groupId: 'user-service-group'
  });

  // Create dead letter queue strategy
  const strategy = new DeadLetterQueueStrategy<UserEvent, UserEventHeaders>({
    deadLetterTopic: 'user-dlq'
  });

  const processor = new EventProcessor(eventBus, {
    retries: 0, // No retries, go straight to DLQ
    maxParallelism: 1,
    strategy: strategy
  } as any);

  await eventBus.connect();
  
  // Add handler that will fail for demonstration
  processor.addHandler('user.created', async (event: EventBase<UserEvent, UserEventHeaders>) => {
    console.log('Processing user created event:', event.message);
    
    // Simulate processing failure
    if (event.message.userId === 'fail-user') {
      throw new Error('Simulated processing failure');
    }
    
    console.log('User created successfully:', event.message.userId);
  });

  await processor.start();
  
  // Dispatch events
  const dispatcher = new EventDispatcher(eventBus as any);
  
  // This will succeed
  await dispatcher.dispatch({
    id: 'user-123',
    type: 'user.created',
    data: { userId: 'user-123', action: 'created', timestamp: new Date() },
    correlationId: 'corr-123',
    source: 'user-service',
    timestamp: new Date()
  });
  
  // This will fail and go to DLQ
  await dispatcher.dispatch({
    id: 'user-fail',
    type: 'user.created',
    data: { userId: 'fail-user', action: 'created', timestamp: new Date() },
    correlationId: 'corr-fail',
    source: 'user-service',
    timestamp: new Date()
  });
  
  await processor.shutdown();
  await eventBus.disconnect();
}

/**
 * Example 2: Retry with Exponential Backoff Strategy
 * 
 * This strategy implements retry logic with exponential backoff.
 */
export async function retryBackoffExample() {
  console.log('=== Retry with Exponential Backoff Strategy Example ===');
  
  const eventBus = new KafkaEventBus<UserEvent, UserEventHeaders>({
    brokers: ['localhost:9092'],
    topicName: 'user-events',
    groupId: 'user-service-group'
  });

  // Create retry strategy with exponential backoff
  const strategy = new RetryWithBackoffStrategy<UserEvent, UserEventHeaders>({
    maxRetries: 3,
    baseDelay: 1000, // 1 second base delay
    retryTopic: 'user-retry',
    deadLetterTopic: 'user-dlq'
  });

  const processor = new EventProcessor(eventBus, {
    retries: 0, // Strategy handles retries
    maxParallelism: 1,
    strategy: strategy
  } as any);

  await eventBus.connect();
  
  let attemptCount = 0;
  
  processor.addHandler('user.updated', async (event: EventBase<UserEvent, UserEventHeaders>) => {
    attemptCount++;
    console.log(`Processing user updated event (attempt ${attemptCount}):`, event.message);
    
    // Simulate intermittent failure
    if (attemptCount < 3) {
      throw new Error(`Simulated failure on attempt ${attemptCount}`);
    }
    
    console.log('User updated successfully:', event.message.userId);
  });

  await processor.start();
  
  const dispatcher = new EventDispatcher(eventBus as any);
  
  await dispatcher.dispatch({
    id: 'user-retry',
    type: 'user.updated',
    data: { userId: 'user-retry', action: 'updated', timestamp: new Date() },
    correlationId: 'corr-retry',
    source: 'user-service',
    timestamp: new Date()
  });
  
  await processor.shutdown();
  await eventBus.disconnect();
}

/**
 * Example 3: Batch Processing Strategy
 * 
 * This strategy provides batch processing capabilities.
 */
export async function batchProcessingExample() {
  console.log('=== Batch Processing Strategy Example ===');
  
  const eventBus = new KafkaEventBus<UserEvent, UserEventHeaders>({
    brokers: ['localhost:9092'],
    topicName: 'user-events',
    groupId: 'user-service-group'
  });

  // Create batch processing strategy
  const strategy = new BatchProcessingStrategy<UserEvent, UserEventHeaders>({
    batchSize: 5,
    batchTimeout: 2000, // 2 seconds
    deadLetterTopic: 'user-dlq'
  });

  const processor = new EventProcessor(eventBus, {
    retries: 0,
    maxParallelism: 1,
    strategy: strategy
  } as any);

  await eventBus.connect();
  
  processor.addHandler('user.batch', async (event: EventBase<UserEvent, UserEventHeaders>) => {
    console.log('Processing user in batch:', event.message.userId);
    // Simulate some processing time
    await new Promise(resolve => setTimeout(resolve, 100));
  });

  await processor.start();
  
  const dispatcher = new EventDispatcher(eventBus as any);
  
  // Dispatch multiple events that will be processed in batches
  for (let i = 1; i <= 7; i++) {
    await dispatcher.dispatch({
      id: `user-batch-${i}`,
      type: 'user.batch',
      data: { userId: `user-batch-${i}`, action: 'created', timestamp: new Date() },
      correlationId: `corr-batch-${i}`,
      source: 'user-service',
      timestamp: new Date()
    });
  }
  
  await processor.shutdown();
  await eventBus.disconnect();
}

/**
 * Example 4: Priority Queue Strategy
 * 
 * This strategy handles messages with different priority levels.
 */
export async function priorityQueueExample() {
  console.log('=== Priority Queue Strategy Example ===');
  
  const eventBus = new KafkaEventBus<UserEvent, UserEventHeaders>({
    brokers: ['localhost:9092'],
    topicName: 'user-events',
    groupId: 'user-service-group'
  });

  // Create priority queue strategy
  const strategy = new PriorityQueueStrategy<UserEvent, UserEventHeaders>({
    maxPriority: 10,
    deadLetterTopic: 'user-dlq'
  });

  const processor = new EventProcessor(eventBus, {
    retries: 0,
    maxParallelism: 1,
    strategy: strategy
  } as any);

  await eventBus.connect();
  
  processor.addHandler('user.priority', async (event: EventBase<UserEvent, UserEventHeaders>) => {
    console.log('Processing user with priority:', event.message.userId);
  });

  await processor.start();
  
  const dispatcher = new EventDispatcher(eventBus as any);
  
  // Dispatch events with different priorities
  const events = [
    { priority: 1, userId: 'low-priority-user' },
    { priority: 5, userId: 'medium-priority-user' },
    { priority: 10, userId: 'high-priority-user' },
    { priority: 3, userId: 'another-low-priority-user' }
  ];
  
  for (const event of events) {
    await dispatcher.dispatch({
      id: `user-priority-${event.userId}`,
      type: 'user.priority',
      data: { userId: event.userId, action: 'created', timestamp: new Date() },
      correlationId: `corr-priority-${event.userId}`,
      source: 'user-service',
      timestamp: new Date()
    });
  }
  
  await processor.shutdown();
  await eventBus.disconnect();
}

/**
 * Example 5: TTL (Time-To-Live) Strategy
 * 
 * This strategy automatically expires messages that are too old.
 */
export async function ttlStrategyExample() {
  console.log('=== TTL Strategy Example ===');
  
  const eventBus = new KafkaEventBus<UserEvent, UserEventHeaders>({
    brokers: ['localhost:9092'],
    topicName: 'user-events',
    groupId: 'user-service-group'
  });

  // Create TTL strategy
  const strategy = new TTLStrategy<UserEvent, UserEventHeaders>({
    defaultTTL: 5000, // 5 seconds
    deadLetterTopic: 'user-dlq'
  });

  const processor = new EventProcessor(eventBus, {
    retries: 0,
    maxParallelism: 1,
    strategy: strategy
  } as any);

  await eventBus.connect();
  
  processor.addHandler('user.ttl', async (event: EventBase<UserEvent, UserEventHeaders>) => {
    console.log('Processing user with TTL:', event.message.userId);
  });

  await processor.start();
  
  const dispatcher = new EventDispatcher(eventBus as any);
  
  // Dispatch a fresh event
  await dispatcher.dispatch({
    id: 'user-fresh',
    type: 'user.ttl',
    data: { userId: 'user-fresh', action: 'created', timestamp: new Date() },
    correlationId: 'corr-fresh',
    source: 'user-service',
    timestamp: new Date()
  });
  
  // Dispatch an old event (simulate by setting old timestamp)
  const oldTimestamp = new Date(Date.now() - 10000); // 10 seconds ago
  await dispatcher.dispatch({
    id: 'user-old',
    type: 'user.ttl',
    data: { userId: 'user-old', action: 'created', timestamp: oldTimestamp },
    correlationId: 'corr-old',
    source: 'user-service',
    timestamp: oldTimestamp
  });
  
  await processor.shutdown();
  await eventBus.disconnect();
}

/**
 * Example 6: Circuit Breaker Strategy
 * 
 * This strategy implements the circuit breaker pattern.
 */
export async function circuitBreakerExample() {
  console.log('=== Circuit Breaker Strategy Example ===');
  
  const eventBus = new KafkaEventBus<UserEvent, UserEventHeaders>({
    brokers: ['localhost:9092'],
    topicName: 'user-events',
    groupId: 'user-service-group'
  });

  // Create circuit breaker strategy
  const strategy = new CircuitBreakerStrategy<UserEvent, UserEventHeaders>({
    failureThreshold: 3,
    recoveryTimeout: 10000, // 10 seconds
    deadLetterTopic: 'user-dlq'
  });

  const processor = new EventProcessor(eventBus, {
    retries: 0,
    maxParallelism: 1,
    strategy: strategy
  } as any);

  await eventBus.connect();
  
  let failureCount = 0;
  
  processor.addHandler('user.circuit', async (event: EventBase<UserEvent, UserEventHeaders>) => {
    console.log('Processing user with circuit breaker:', event.message.userId);
    
    // Simulate failures to trigger circuit breaker
    if (failureCount < 5) {
      failureCount++;
      throw new Error(`Simulated failure ${failureCount}`);
    }
    
    console.log('User processed successfully after circuit breaker recovery');
  });

  await processor.start();
  
  const dispatcher = new EventDispatcher(eventBus as any);
  
  // Dispatch events that will trigger the circuit breaker
  for (let i = 1; i <= 6; i++) {
    await dispatcher.dispatch({
      id: `user-circuit-${i}`,
      type: 'user.circuit',
      data: { userId: `user-circuit-${i}`, action: 'created', timestamp: new Date() },
      correlationId: `corr-circuit-${i}`,
      source: 'user-service',
      timestamp: new Date()
    });
    
    // Wait a bit between events
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  
  await processor.shutdown();
  await eventBus.disconnect();
}

/**
 * Run all examples
 */
export async function runAllExamples() {
  try {
    await deadLetterQueueExample();
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    await retryBackoffExample();
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    await batchProcessingExample();
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    await priorityQueueExample();
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    await ttlStrategyExample();
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    await circuitBreakerExample();
    
    console.log('All examples completed successfully!');
  } catch (error) {
    console.error('Error running examples:', error);
  }
}
