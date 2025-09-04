import { KafkaEventBus } from '../src/kafka-event-bus';
import { EventProcessor, EventDispatcher, EventBase, ExternalEvent } from '@soapjs/soap';

// Example usage of the Kafka event system

interface UserEvent {
  userId: string;
  action: string;
  timestamp: Date;
}

interface UserEventHeaders {
  correlation_id: string;
  timestamp: string;
  source: string;
}

async function example() {
  // Create the event system
  const eventBus = new KafkaEventBus<UserEvent, UserEventHeaders>({
    brokers: ['localhost:9092'],
    topicName: 'user-events',
    groupId: 'user-service-group'
  });

  const dispatcher = new EventDispatcher(eventBus as any, {
    maxRetries: 3,
    retryDelay: 1000,
    callbacks: {
      onSuccess: (externalEvent) => console.log(`Event ${externalEvent.type} dispatched successfully`),
      onError: (error, externalEvent) => console.error(`Failed to dispatch event ${externalEvent.type}:`, error)
    }
  });

  const processor = new EventProcessor(eventBus, {
    retries: 3,
    maxParallelism: 5,
    callbacks: {
      onSuccess: (event) => console.log('Event processed successfully:', event),
      onError: (error, event) => console.error('Event processing failed:', error, event)
    }
  });

  try {
    // Connect to Kafka
    await eventBus.connect();
    console.log('Connected to Kafka');

    // Set up event processor with multiple handlers
    processor.addHandler('user.created', async (payload: any) => {
      console.log('Processing user created event:', payload);
      // Simulate some processing
      await new Promise(resolve => setTimeout(resolve, 100));
    });

    processor.addHandler('user.updated', async (payload: any) => {
      console.log('Processing user updated event:', payload);
      // Simulate some processing
      await new Promise(resolve => setTimeout(resolve, 100));
    });

    await processor.start();

    // Dispatch events
    const userCreatedEvent: UserEvent = {
      userId: 'user-123',
      action: 'created',
      timestamp: new Date()
    };

    const userUpdatedEvent: UserEvent = {
      userId: 'user-123',
      action: 'updated',
      timestamp: new Date()
    };

    const userCreatedExternalEvent: ExternalEvent = {
      id: 'user-created-123',
      type: 'user.created',
      data: userCreatedEvent as unknown as Record<string, unknown>,
      correlationId: 'corr-123',
      source: 'user-service',
      timestamp: new Date()
    };

    const userUpdatedExternalEvent: ExternalEvent = {
      id: 'user-updated-124',
      type: 'user.updated',
      data: userUpdatedEvent as unknown as Record<string, unknown>,
      correlationId: 'corr-124',
      source: 'user-service',
      timestamp: new Date()
    };

    await dispatcher.dispatch(userCreatedExternalEvent);
    await dispatcher.dispatch(userUpdatedExternalEvent);

    // Subscribe to pattern-based events
    const patternSubscriptionId = await eventBus.subscribeToPattern(
      'user.*',
      (eventId, event) => {
        console.log(`Pattern match - Event: ${eventId}`, event);
      }
    );

    // Subscribe to batch events
    const batchSubscriptionId = await eventBus.subscribeBatch(
      'user.batch',
      (events) => {
        console.log(`Batch processing ${events.length} events:`, events);
      }
    );

    // Wait for some time to process events
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Clean up
    await eventBus.unsubscribe(patternSubscriptionId);
    await eventBus.unsubscribe(batchSubscriptionId);
    await processor.shutdown();
    await eventBus.disconnect();

    console.log('Example completed successfully');

  } catch (error) {
    console.error('Example failed:', error);
  }
}

// Health check example
async function healthCheckExample() {
  const eventBus = new KafkaEventBus<UserEvent, UserEventHeaders>({
    brokers: ['localhost:9092']
  });

  try {
    await eventBus.connect();
    
    const isHealthy = await eventBus.checkHealth();
    console.log('Kafka health status:', isHealthy ? 'Healthy' : 'Unhealthy');
    
    await eventBus.disconnect();
  } catch (error) {
    console.error('Health check failed:', error);
  }
}

// Error handling example
async function errorHandlingExample() {
  const eventBus = new KafkaEventBus<UserEvent, UserEventHeaders>({
    brokers: ['localhost:9092'],
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
  });

  try {
    await eventBus.connect();

    // Set up error handling
    const processor = new EventProcessor(eventBus, {
      retries: 3,
      dlq: {
        enabled: true,
        topic: 'user-events-dlq'
      },
      callbacks: {
        onError: (error, event) => {
          console.error('Event processing error:', error);
          console.error('Failed event:', event);
        }
      }
    });

    processor.addHandler('user.error', async (payload: any) => {
      // Simulate an error
      throw new Error('Simulated processing error');
    });

    // Dispatch an event that will cause an error
    await eventBus.publish('user.error', {
      message: {
        userId: 'user-123',
        action: 'error',
        timestamp: new Date()
      },
      headers: {
        correlation_id: 'corr-error',
        timestamp: new Date().toISOString(),
        source: 'error-test'
      }
    });

    await new Promise(resolve => setTimeout(resolve, 1000));
    await processor.shutdown();
    await eventBus.disconnect();

  } catch (error) {
    console.error('Error handling example failed:', error);
  }
}



// Export examples for use
export {
  example,
  healthCheckExample,
  errorHandlingExample
};
