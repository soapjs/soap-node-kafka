# @soapjs/soap-node-kafka

This package provides Apache Kafka integration for the SoapJS framework, enabling reliable event-driven architecture with message queuing, event publishing, consuming, and processing capabilities.

## Features

- **Clean Architecture Support**: Follows SoapJS clean architecture patterns with full abstraction support.
- **Type Safety**: Full TypeScript support with comprehensive type definitions.
- **EventBus Implementation**: Full implementation of EventBus interface for Apache Kafka.
- **EventDispatcher Integration**: Seamless integration with SoapJS EventDispatcher for reliable event dispatching.
- **EventProcessor Integration**: Works with SoapJS EventProcessor for robust event processing with retry logic.
- **Pattern-based Subscriptions**: Subscribe to events using Kafka topic patterns (e.g., 'user.*', '*.created').
- **Batch Processing**: Process multiple events efficiently in configurable batches.
- **Connection Management**: Advanced connection management with health monitoring and automatic reconnection.
- **Error Handling**: Comprehensive error handling with retry policies and dead letter topic support.
- **Performance Monitoring**: Built-in performance monitoring with metrics collection and slow operation detection.
- **Partitioning Support**: Full support for Kafka partitioning and consumer groups.
- **Compatibility**: Support for various Kafka versions with feature detection.

## Installation

Remember to have `kafkajs` and `@soapjs/soap` installed in your project in which you want to use this package.

```bash
npm install @soapjs/soap-node-kafka
```

## Quick Start

### 1. Import the necessary classes:

```typescript
import { KafkaEventBus, KafkaEventBusOptions } from '@soapjs/soap-node-kafka';
import { EventProcessor, EventDispatcher, EventBase, ExternalEvent } from '@soapjs/soap';
```

### 2. Set up your Kafka configuration:

```typescript
const eventBusOptions: KafkaEventBusOptions = {
  brokers: ['localhost:9092'],
  topicName: 'myapp-events',
  groupId: 'myapp-service-group',
  retryPolicy: {
    maxRetries: 3,
    delay: 1000,
    backoff: {
      type: 'exponential',
      multiplier: 2,
      maxDelay: 30000,
      jitter: true
    }
  }
};
```

### 3. Create a new `KafkaEventBus` instance:

```typescript
const eventBus = new KafkaEventBus(eventBusOptions);
```

### 4. Set up event processing and dispatching:

```typescript
// Create event processor
const processor = new EventProcessor(eventBus, {
  retries: 3,
  maxParallelism: 5,
  callbacks: {
    onSuccess: (event) => console.log('Event processed successfully:', event),
    onError: (error, event) => console.error('Event processing failed:', error, event)
  }
});

// Create event dispatcher
const dispatcher = new EventDispatcher(eventBus, {
  maxRetries: 3,
  retryDelay: 1000,
  exponentialBackoff: true,
  callbacks: {
    onSuccess: (event) => console.log('Event dispatched successfully:', event),
    onError: (error, event) => console.error('Event dispatch failed:', error, event),
    onRetry: (event, attempt, error) => console.log(`Retrying event (attempt ${attempt}):`, error.message)
  }
});
```

### 5. Connect to Kafka and start processing:

```typescript
// Connect to Kafka
await eventBus.connect();
console.log('Connected to Kafka');

// Set up event handlers
processor.addHandler('user.created', async (payload) => {
  console.log('User created:', payload.message);
  // Process user creation logic
});

processor.addHandler('user.updated', async (payload) => {
  console.log('User updated:', payload.message);
  // Process user update logic
});

// Start the processor
await processor.start();
console.log('Event processor started');
```

### 6. Dispatch events:

```typescript
// Dispatch a user creation event
const userCreatedEvent: ExternalEvent = {
  id: 'user-123',
  type: 'user.created',
  data: { 
    userId: '123', 
    name: 'John Doe',
    email: 'john@example.com'
  },
  correlationId: 'corr-123',
  source: 'user-service',
  timestamp: new Date()
};

await dispatcher.dispatch(userCreatedEvent);

// Dispatch a user update event
const userUpdatedEvent: ExternalEvent = {
  id: 'user-456',
  type: 'user.updated',
  data: { 
    userId: '123', 
    name: 'Jane Doe',
    email: 'jane@example.com'
  },
  correlationId: 'corr-456',
  source: 'user-service',
  timestamp: new Date()
};

await dispatcher.dispatch(userUpdatedEvent);
```

### 7. Clean up resources:

```typescript
// Shutdown processor
await processor.shutdown();

// Disconnect from Kafka
await eventBus.disconnect();
console.log('Disconnected from Kafka');
```

## Advanced Usage

### Pattern-based Subscriptions

Subscribe to multiple events using routing patterns:

```typescript
// Subscribe to all user events
await eventBus.subscribeToPattern('user.*', (eventId, eventData) => {
  console.log(`Received ${eventId}:`, eventData.message);
});

// Subscribe to all creation events
await eventBus.subscribeToPattern('*.created', (eventId, eventData) => {
  console.log(`Something was created: ${eventId}`, eventData.message);
});

// Subscribe to specific pattern combinations
await eventBus.subscribeToPattern('user.*.admin', (eventId, eventData) => {
  console.log(`Admin user event: ${eventId}`, eventData.message);
});
```

### Batch Processing

Process events in batches for better performance:

```typescript
// Process events in batches of 10 or after 1 second timeout
await eventBus.subscribeBatch('user.events', (events) => {
  console.log(`Processing ${events.length} events in batch`);
  
  events.forEach(event => {
    console.log('Processing event:', event.message);
    // Process each event in the batch
  });
  
  // Batch processing logic (e.g., bulk database operations)
  await processBatchEvents(events);
});
```

### Health Monitoring

Monitor connection health and handle reconnections:

```typescript
// Check connection health
const isHealthy = await eventBus.checkHealth();
if (!isHealthy) {
  console.log('Connection is unhealthy, attempting to reconnect...');
  await eventBus.connect();
}

// Set up periodic health checks
setInterval(async () => {
  const health = await eventBus.checkHealth();
  if (!health) {
    console.warn('Kafka connection lost, attempting reconnection...');
    try {
      await eventBus.connect();
      console.log('Reconnected to Kafka');
    } catch (error) {
      console.error('Failed to reconnect:', error);
    }
  }
}, 30000); // Check every 30 seconds
```

### Error Handling and Retry Logic

Configure comprehensive error handling:

```typescript
// Event processor with retry logic
const processor = new EventProcessor(eventBus, {
  retries: 5,
  retryDelay: 2000,
  maxParallelism: 3,
  callbacks: {
    onSuccess: (event) => {
      console.log('Event processed successfully:', event);
    },
    onError: (error, event) => {
      console.error('Event processing failed after all retries:', error);
      console.error('Failed event:', event);
      
      // Send to dead letter queue or alert system
      sendToDeadLetterQueue(event, error);
    }
  }
});

// Event dispatcher with exponential backoff
const dispatcher = new EventDispatcher(eventBus, {
  maxRetries: 3,
  retryDelay: 1000,
  exponentialBackoff: true,
  callbacks: {
    onRetry: (event, attempt, error) => {
      console.log(`Retrying dispatch (attempt ${attempt}/${3}):`, error.message);
    },
    onError: (error, event) => {
      console.error('Event dispatch failed after all retries:', error);
      // Handle final failure
    }
  }
});
```

### Custom Event Handlers

Create sophisticated event handling logic:

```typescript
// User service with event handling
class UserService {
  constructor(
    private eventBus: KafkaEventBus,
    private processor: EventProcessor
  ) {}

  async initialize() {
    // Set up event handlers
    this.processor.addHandler('user.created', this.handleUserCreated.bind(this));
    this.processor.addHandler('user.updated', this.handleUserUpdated.bind(this));
    this.processor.addHandler('user.deleted', this.handleUserDeleted.bind(this));
    
    await this.processor.start();
  }

  private async handleUserCreated(payload: EventBase<any, any>) {
    try {
      const userData = payload.message;
      console.log('Processing user creation:', userData);
      
      // Business logic for user creation
      await this.createUserInDatabase(userData);
      await this.sendWelcomeEmail(userData);
      await this.setupUserPreferences(userData);
      
      console.log('User created successfully:', userData.userId);
    } catch (error) {
      console.error('Failed to process user creation:', error);
      throw error; // This will trigger retry logic
    }
  }

  private async handleUserUpdated(payload: EventBase<any, any>) {
    const userData = payload.message;
    console.log('Processing user update:', userData);
    
    // Update user in database
    await this.updateUserInDatabase(userData);
    
    // Notify other services
    await this.notifyUserUpdate(userData);
  }

  private async handleUserDeleted(payload: EventBase<any, any>) {
    const userData = payload.message;
    console.log('Processing user deletion:', userData);
    
    // Clean up user data
    await this.cleanupUserData(userData.userId);
    await this.archiveUserData(userData.userId);
  }

  // Helper methods
  private async createUserInDatabase(userData: any) {
    // Database logic
  }

  private async sendWelcomeEmail(userData: any) {
    // Email logic
  }

  private async setupUserPreferences(userData: any) {
    // Preferences logic
  }

  private async updateUserInDatabase(userData: any) {
    // Update logic
  }

  private async notifyUserUpdate(userData: any) {
    // Notification logic
  }

  private async cleanupUserData(userId: string) {
    // Cleanup logic
  }

  private async archiveUserData(userId: string) {
    // Archive logic
  }
}
```

## API Reference

### Core Classes

- **KafkaEventBus**: Main event bus implementation for Kafka connections and message routing
- **KafkaEventBusOptions**: Configuration options for the event bus
- **EventProcessor**: Event processing with retry logic and parallelism control (from @soapjs/soap)
- **EventDispatcher**: Event dispatching with retry logic and error handling (from @soapjs/soap)

### Configuration Classes

- **KafkaEventBusOptions**: Configuration for Kafka connection and behavior
- **BackoffOptions**: Configuration for retry backoff strategies

### Interfaces

- **EventBus**: Core event bus interface (from @soapjs/soap)
- **EventBase**: Base event structure with message, headers, and error fields
- **ExternalEvent**: External event structure for dispatching
- **EventDispatcherOptions**: Options for event dispatcher configuration
- **EventProcessorOptions**: Options for event processor configuration

## Error Handling

The package provides comprehensive error handling with specific Kafka error types:

```typescript
try {
  await eventBus.publish('user.created', {
    message: { userId: '123', name: 'John Doe' },
    headers: { correlation_id: 'corr-123', timestamp: new Date().toISOString() }
  });
} catch (error) {
  if (error.message.includes('Not connected to Kafka')) {
    console.error('Connection error:', error.message);
    // Attempt to reconnect
    await eventBus.connect();
  } else if (error.message.includes('Topic creation errors')) {
    console.error('Topic error:', error.message);
    // Handle topic issues
  } else {
    console.error('Unexpected error:', error);
  }
}
```

## Testing

### Unit Tests

Run unit tests (mocked Kafka):

```bash
npm run test:unit
```

### Integration Tests

Integration tests use **Testcontainers** to automatically start and manage Kafka containers for testing.

#### Prerequisites

1. **Docker**: Ensure Docker is running on your system
2. **Testcontainers**: Automatically manages Kafka containers
3. **No manual setup required**: Everything is handled automatically

#### Running Integration Tests

```bash
# Run only integration tests (requires Docker)
npm run test:integration

# Run all tests (unit + integration)
npm test
```

#### Integration Test Coverage

Integration tests cover:

- **EventBus Operations**: Publishing, subscribing, pattern matching, batch processing
- **EventProcessor Integration**: Event processing, retry logic, parallelism
- **EventDispatcher Integration**: Event dispatching, retry logic, error handling
- **Error Scenarios**: Connection failures, malformed messages, retry logic
- **Real-world Scenarios**: End-to-end event flows, high throughput, concurrent access

#### Test Environment

Integration tests use:

- **Isolated Containers**: Each test run gets a fresh Kafka container
- **Automatic Management**: Containers are started/stopped automatically
- **Real Kafka**: Actual message broker operations (no mocking)
- **Timeout Handling**: 30-second timeout for slow operations
- **Clean State**: Topics and consumer groups cleaned before each test

### Test Coverage

The package includes comprehensive test coverage:
- **Unit Tests**: 91.9% code coverage with mocked dependencies
- **Integration Tests**: Full end-to-end scenarios with real Kafka
- **Error Scenarios**: Connection failures, malformed messages, retry logic
- **Performance Tests**: High throughput, parallelism, batch processing

## Performance Optimization

### Connection Pool Configuration

```typescript
const eventBusOptions: KafkaEventBusOptions = {
  brokers: ['localhost:9092'],
  topicName: 'myapp-events',
  groupId: 'myapp-service-group',
  retryPolicy: {
    maxRetries: 5,
    delay: 1000,
    backoff: {
      type: 'exponential',
      multiplier: 2,
      maxDelay: 30000,
      jitter: true
    }
  }
};
```

### Batch Processing Optimization

```typescript
// Optimize batch processing for high throughput
await eventBus.subscribeBatch('high-volume.events', (events) => {
  // Process in larger batches for better performance
  processBatchEvents(events);
}, {
  batchSize: 50,        // Larger batch size
  batchTimeout: 500     // Shorter timeout for faster processing
});
```

### Parallelism Configuration

```typescript
// Configure processor for optimal parallelism
const processor = new EventProcessor(eventBus, {
  retries: 3,
  maxParallelism: 10,   // Increase for high-throughput scenarios
  callbacks: {
    onSuccess: (event) => console.log('Processed:', event),
    onError: (error, event) => console.error('Failed:', error)
  }
});
```

## Security Best Practices

### Connection Security

```typescript
// Use environment variables for sensitive data
const eventBusOptions: KafkaEventBusOptions = {
  brokers: process.env.KAFKA_BROKERS?.split(',') || ['localhost:9092'],
  topicName: process.env.KAFKA_TOPIC || 'myapp-events',
  groupId: process.env.KAFKA_GROUP_ID || 'myapp-service-group'
};
```

### Message Validation

```typescript
// Validate incoming messages
processor.addHandler('user.created', async (payload) => {
  // Validate message structure
  if (!payload.message || !payload.message.userId) {
    throw new Error('Invalid message structure');
  }
  
  // Validate message content
  if (typeof payload.message.userId !== 'string') {
    throw new Error('Invalid userId type');
  }
  
  // Process validated message
  await processUserCreation(payload.message);
});
```

## Troubleshooting

### Common Issues

1. **Connection Issues**
   ```typescript
   // Check connection health
   const isHealthy = await eventBus.checkHealth();
   if (!isHealthy) {
     console.log('Connection unhealthy, attempting reconnect...');
     await eventBus.connect();
   }
   ```

2. **Performance Issues**
   ```typescript
   // Monitor event processing
   const processor = new EventProcessor(eventBus, {
     maxParallelism: 5,  // Reduce if experiencing high CPU usage
     retries: 3,
     callbacks: {
       onError: (error, event) => {
         console.log('Processing error:', error.message);
         // Log performance metrics
       }
     }
   });
   ```

3. **Memory Issues**
   ```typescript
   // Use batch processing for high-volume events
   await eventBus.subscribeBatch('high-volume.events', (events) => {
     // Process in batches to reduce memory usage
     processBatchEvents(events);
   });
   ```

### Debug Mode

```typescript
// Enable detailed logging
const eventBus = new KafkaEventBus({
  brokers: ['localhost:9092'],
  topicName: 'myapp-events',
  groupId: 'myapp-service-group'
});

// Add debug logging to event handlers
processor.addHandler('debug.events', async (payload) => {
  console.log('Debug - Event received:', {
    message: payload.message,
    headers: payload.headers,
    timestamp: new Date().toISOString()
  });
});
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- **Documentation**: [https://docs.soapjs.com](https://docs.soapjs.com)
- **Issues**: [GitHub Issues](https://github.com/soapjs/soap-node-kafka/issues)
- **Discussions**: [GitHub Discussions](https://github.com/soapjs/soap-node-kafka/discussions)
- **Email**: radoslaw.kamysz@gmail.com