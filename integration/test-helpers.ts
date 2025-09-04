import { KafkaEventBus } from '../src/kafka-event-bus';
import { EventProcessor, EventDispatcher, EventBase } from '@soapjs/soap';

export interface TestEventSystem {
  eventBus: any;
  dispatcher: any;
  processor: any;
}

export class IntegrationTestHelper {
  static async createTestEventSystem(
    brokers: string[],
    options?: {
      topicName?: string;
      groupId?: string;
      maxRetries?: number;
      retryDelay?: number;
      maxParallelism?: number;
    }
  ): Promise<TestEventSystem> {
    const eventBus = new KafkaEventBus<string, Record<string, unknown>>({
      brokers,
      topicName: options?.topicName || 'test-events',
      groupId: options?.groupId || 'test-group'
    });

    const dispatcher = new EventDispatcher(eventBus, {
      maxRetries: options?.maxRetries || 3,
      retryDelay: options?.retryDelay || 100
    });

    const processor = new EventProcessor(eventBus, {
      retries: options?.maxRetries || 3,
      maxParallelism: options?.maxParallelism || 2
    });

    return {
      eventBus,
      dispatcher,
      processor
    };
  }

  static async waitForConnection(eventBus: any, timeoutMs: number = 5000): Promise<void> {
    const startTime = Date.now();
    while (Date.now() - startTime < timeoutMs) {
      try {
        await eventBus.connect();
        const isHealthy = await eventBus.checkHealth();
        if (isHealthy) {
          return;
        }
      } catch (error) {
        // Continue waiting
      }
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    throw new Error(`Failed to establish connection within ${timeoutMs}ms`);
  }

  static async waitForSubscription(
    eventBus: any,
    eventId: string,
    handler: (event: EventBase<string, Record<string, unknown>>) => void,
    timeoutMs: number = 5000
  ): Promise<void> {
    const startTime = Date.now();
    while (Date.now() - startTime < timeoutMs) {
      try {
        await eventBus.subscribe(eventId, handler);
        return;
      } catch (error) {
        // Continue waiting
      }
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    throw new Error(`Failed to establish subscription within ${timeoutMs}ms`);
  }

  static async waitForProcessing(
    processor: any,
    eventId: string,
    handler: (payload: string) => Promise<void>,
    timeoutMs: number = 5000
  ): Promise<void> {
    const startTime = Date.now();
    while (Date.now() - startTime < timeoutMs) {
      try {
        await processor.start(eventId, handler);
        return;
      } catch (error) {
        // Continue waiting
      }
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    throw new Error(`Failed to start processing within ${timeoutMs}ms`);
  }

  static async waitForEvents(
    expectedCount: number,
    receivedEvents: any[],
    timeoutMs: number = 10000
  ): Promise<void> {
    const startTime = Date.now();
    while (Date.now() - startTime < timeoutMs) {
      if (receivedEvents.length >= expectedCount) {
        return;
      }
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    throw new Error(`Expected ${expectedCount} events, but received ${receivedEvents.length} within ${timeoutMs}ms`);
  }

  static async waitForBatches(
    expectedCount: number,
    receivedBatches: any[],
    timeoutMs: number = 10000
  ): Promise<void> {
    const startTime = Date.now();
    while (Date.now() - startTime < timeoutMs) {
      if (receivedBatches.length >= expectedCount) {
        return;
      }
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    throw new Error(`Expected ${expectedCount} batches, but received ${receivedBatches.length} within ${timeoutMs}ms`);
  }

  static async cleanupEventSystem(system: TestEventSystem): Promise<void> {
    try {
      if (system.processor) {
        await system.processor.shutdown();
      }
      if (system.eventBus) {
        await system.eventBus.disconnect();
      }
    } catch (error) {
      console.warn('Error during cleanup:', error);
    }
  }

  static generateTestEvent(
    message: string,
    correlationId?: string,
    additionalHeaders?: Record<string, unknown>
  ): EventBase<string, Record<string, unknown>> {
    return {
      message,
      headers: {
        correlation_id: correlationId || `test-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        timestamp: new Date().toISOString(),
        source: 'integration-test',
        ...additionalHeaders
      }
    };
  }

  static generateTestEvents(
    count: number,
    messagePrefix: string = 'Test message'
  ): EventBase<string, Record<string, unknown>>[] {
    return Array.from({ length: count }, (_, index) => 
      this.generateTestEvent(`${messagePrefix} ${index + 1}`)
    );
  }

  static async measurePerformance<T>(
    operation: () => Promise<T>,
    operationName: string = 'Operation'
  ): Promise<{ result: T; duration: number; eventsPerSecond?: number }> {
    const startTime = Date.now();
    const result = await operation();
    const endTime = Date.now();
    const duration = endTime - startTime;

    console.log(`${operationName} completed in ${duration}ms`);

    return { result, duration };
  }

  static async simulateLoad(
    dispatcher: any,
    eventId: string,
    eventCount: number,
    batchSize: number = 10,
    delayBetweenBatches: number = 100
  ): Promise<void> {
    const events = this.generateTestEvents(eventCount, 'Load test message');
    
    for (let i = 0; i < events.length; i += batchSize) {
      const batch = events.slice(i, i + batchSize);
      
      // Dispatch batch
      for (const event of batch) {
        await dispatcher.dispatch(eventId, event.message, event.headers);
      }
      
      // Wait between batches
      if (i + batchSize < events.length) {
        await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
      }
    }
  }

  static async waitForHealthCheck(
    eventBus: any,
    expectedHealth: boolean = true,
    timeoutMs: number = 5000
  ): Promise<void> {
    const startTime = Date.now();
    while (Date.now() - startTime < timeoutMs) {
      try {
        const isHealthy = await eventBus.checkHealth();
        if (isHealthy === expectedHealth) {
          return;
        }
      } catch (error) {
        if (!expectedHealth) {
          return; // Expected error
        }
      }
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    throw new Error(`Health check did not return ${expectedHealth} within ${timeoutMs}ms`);
  }

  static async retryOperation<T>(
    operation: () => Promise<T>,
    maxRetries: number = 3,
    delayMs: number = 1000
  ): Promise<T> {
    let lastError: Error;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error as Error;
        if (attempt < maxRetries) {
          await new Promise(resolve => setTimeout(resolve, delayMs));
        }
      }
    }
    
    throw lastError!;
  }
}
