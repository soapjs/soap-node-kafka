# Kafka Processing Strategies

This document describes the Kafka-specific processing strategies available in the `@soapjs/soap-node-kafka` package. These strategies leverage Kafka's advanced features to provide robust message processing capabilities. We encourage you to write your own custom strategies tailored to your specific needs - the provided strategies cover basic common scenarios, but you can extend the base `KafkaProcessingStrategy` class to implement more sophisticated behaviors.



## Overview

The package provides several specialized processing strategies that extend the default SOAP framework behavior with Kafka-specific features:

- **DeadLetterQueueStrategy** - Routes failed messages to dead letter topics
- **RetryWithBackoffStrategy** - Implements exponential backoff retry logic
- **BatchProcessingStrategy** - Processes messages in batches for better throughput
- **PriorityQueueStrategy** - Handles messages based on priority levels
- **TTLStrategy** - Automatically expires old messages
- **CircuitBreakerStrategy** - Implements circuit breaker pattern for fault tolerance

## Quick Start

```typescript
import { 
  KafkaEventBus, 
  DeadLetterQueueStrategy,
  RetryWithBackoffStrategy,
  BatchProcessingStrategy,
  PriorityQueueStrategy,
  TTLStrategy,
  CircuitBreakerStrategy
} from '@soapjs/soap-node-kafka';
import { EventProcessor } from '@soapjs/soap';

// Create event bus
const eventBus = new KafkaEventBus({
  brokers: ['localhost:9092'],
  topicName: 'my-events',
  groupId: 'my-service-group'
});

// Create a retry strategy with exponential backoff
const strategy = new RetryWithBackoffStrategy({
  maxRetries: 3,
  baseDelay: 1000,
  retryTopic: 'retry-topic',
  deadLetterTopic: 'dlq-topic'
});

// Use strategy with EventProcessor
const processor = new EventProcessor(eventBus, {
  retries: 0, // Strategy handles retries
  maxParallelism: 5,
  strategy: strategy
});
```

## Available Strategies

### 1. Dead Letter Queue Strategy

Routes failed messages to a dead letter topic for manual inspection and recovery.

```typescript
const strategy = new DeadLetterQueueStrategy({
  deadLetterTopic: 'my-dlq'
});
```

**Features:**
- Automatic routing of failed messages to DLQ
- Preserves original message with failure reason
- Enables manual message recovery and debugging

**Use Cases:**
- Error handling and debugging
- Message recovery workflows
- Compliance and audit requirements

### 2. Retry with Exponential Backoff Strategy

Implements retry logic with exponential backoff using Kafka's message delay and dead letter topic features.

```typescript
const strategy = new RetryWithBackoffStrategy({
  maxRetries: 5,
  baseDelay: 1000,
  retryTopic: 'retry-topic',
  deadLetterTopic: 'dlq-topic'
});
```

**Features:**
- Exponential backoff delays (1s, 2s, 4s, 8s, 16s...)
- Configurable maximum retry attempts
- Automatic routing to DLQ after max retries
- Preserves retry count in message headers

**Use Cases:**
- Handling transient failures
- Rate limiting and backpressure
- Service recovery scenarios

### 3. Batch Processing Strategy

Processes messages in batches to improve throughput and reduce processing overhead.

```typescript
const strategy = new BatchProcessingStrategy({
  batchSize: 20,
  batchTimeout: 2000,
  deadLetterTopic: 'dlq-topic'
});
```

**Features:**
- Configurable batch size and timeout
- Automatic batch processing when size or timeout reached
- Parallel processing of batch messages
- Failed batch messages routed to DLQ

**Use Cases:**
- High-throughput message processing
- Database bulk operations
- API rate limiting compliance

### 4. Priority Queue Strategy

Processes messages based on priority levels, ensuring high-priority messages are handled first.

```typescript
const strategy = new PriorityQueueStrategy({
  maxPriority: 10,
  deadLetterTopic: 'dlq-topic'
});
```

**Features:**
- Priority-based message ordering
- Configurable maximum priority level
- Automatic priority queue management
- Failed messages routed to DLQ

**Use Cases:**
- Critical vs. non-critical message handling
- SLA compliance for high-priority events
- Resource allocation based on importance

### 5. TTL (Time-To-Live) Strategy

Automatically expires messages that are too old, preventing stale messages from being processed.

```typescript
const strategy = new TTLStrategy({
  defaultTTL: 300000, // 5 minutes
  deadLetterTopic: 'dlq-topic'
});
```

**Features:**
- Configurable default TTL
- Per-message TTL override via headers
- Automatic message age calculation
- Expired messages routed to DLQ

**Use Cases:**
- Time-sensitive message processing
- Preventing stale data processing
- Compliance with data retention policies

### 6. Circuit Breaker Strategy

Implements the circuit breaker pattern to prevent cascading failures and provide graceful degradation.

```typescript
const strategy = new CircuitBreakerStrategy({
  failureThreshold: 5,
  recoveryTimeout: 60000, // 1 minute
  deadLetterTopic: 'dlq-topic'
});
```

**Features:**
- Configurable failure threshold
- Automatic circuit state management (CLOSED → OPEN → HALF_OPEN)
- Recovery timeout for circuit reset
- Failed messages routed to DLQ during OPEN state

**Use Cases:**
- Preventing cascading failures
- Service degradation handling
- Fault tolerance and resilience

## Composite Strategies

You can combine multiple strategies for more sophisticated message processing by creating custom strategies that extend the base `KafkaProcessingStrategy` class and implement multiple behaviors.

## Configuration Options

Each strategy has its own configuration interface. Refer to the individual strategy documentation above for specific configuration options.

## Best Practices

### 1. Topic Setup

Ensure your Kafka topics are properly configured:

```typescript
// Set up dead letter topic
await admin.createTopics({
  topics: [{
    topic: 'dlq-topic',
    numPartitions: 3,
    replicationFactor: 1
  }]
});

// Set up retry topic
await admin.createTopics({
  topics: [{
    topic: 'retry-topic',
    numPartitions: 3,
    replicationFactor: 1
  }]
});
```

### 2. Consumer Group Configuration

Configure consumer groups with appropriate settings:

```typescript
const consumer = kafka.consumer({ 
  groupId: 'my-service-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000
});
```

### 3. Error Handling

Always provide a dead letter topic for error handling:

```typescript
const strategy = new RetryWithBackoffStrategy({
  maxRetries: 3,
  deadLetterTopic: 'dlq-topic' // Always provide DLQ
});
```

### 4. Monitoring

Monitor your dead letter topics for failed messages:

```typescript
// Subscribe to dead letter topic for monitoring
await eventBus.subscribe('dead-letter', (message) => {
  console.error('Message failed processing:', message);
  // Send alert, log to monitoring system, etc.
});
```

## Examples

See the `strategy-examples.ts` file for complete working examples of each strategy.

## Migration from Default Strategy

To migrate from the default SOAP strategy to Kafka-specific strategies:

1. **Identify your requirements** - What Kafka features do you need?
2. **Choose appropriate strategy** - Create the right strategy class directly
3. **Configure topics** - Set up required Kafka topics
4. **Update EventProcessor** - Pass the strategy to your EventProcessor
5. **Test thoroughly** - Ensure your application handles the new behavior correctly

```typescript
// Before (default strategy)
const processor = new EventProcessor(eventBus, {
  retries: 3,
  maxParallelism: 5
});

// After (Kafka strategy)
const strategy = new RetryWithBackoffStrategy({
  maxRetries: 3,
  baseDelay: 1000
});

const processor = new EventProcessor(eventBus, {
  retries: 0, // Strategy handles retries
  maxParallelism: 5,
  strategy: strategy
});
```

## Troubleshooting

### Common Issues

1. **Missing Topics**: Ensure all required topics are created
2. **Consumer Group Configuration**: Check consumer group settings
3. **Message Headers**: Verify custom headers are properly set
4. **TTL Calculation**: Ensure timestamp headers are in correct format

### Debug Mode

Enable debug logging to troubleshoot strategy behavior:

```typescript
const strategy = new RetryWithBackoffStrategy({
  maxRetries: 3,
  baseDelay: 1000
});

// Add debug logging
strategy.setChannel(channel);
```

## Performance Considerations

- **Batch Processing**: Use for high-throughput scenarios
- **Priority Queues**: Use sparingly as they can impact performance
- **TTL Strategy**: Consider message volume when setting TTL values
- **Circuit Breaker**: Monitor failure rates to tune thresholds

## Security Considerations

- **Dead Letter Topics**: Ensure DLQ access is properly secured
- **Message Headers**: Validate and sanitize custom headers
- **Topic Permissions**: Use appropriate Kafka permissions
- **Retry Limits**: Set reasonable retry limits to prevent abuse
