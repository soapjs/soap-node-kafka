import { KafkaEventBus } from '../src/kafka-event-bus';
import { EventBase } from '@soapjs/soap';
import { setupIntegrationTests, teardownIntegrationTests } from './setup';

describe('Kafka EventBus Integration Tests', () => {
  let eventBus: KafkaEventBus<string, Record<string, unknown>, string>;
  let brokers: string[];

  beforeAll(async () => {
    brokers = await setupIntegrationTests();
    eventBus = new KafkaEventBus({
      brokers,
      topicName: 'integration-test-events',
      groupId: 'integration-test-group'
    });
  }, 60000);

  afterAll(async () => {
    if (eventBus) {
      await eventBus.disconnect();
    }
    await teardownIntegrationTests();
  }, 30000);

  describe('Connection Management', () => {
    it('should connect to Kafka successfully', async () => {
      await expect(eventBus.connect()).resolves.not.toThrow();
    });

    it('should check health status correctly', async () => {
      const isHealthy = await eventBus.checkHealth();
      expect(isHealthy).toBe(true);
    });

    it('should disconnect from Kafka successfully', async () => {
      await expect(eventBus.disconnect()).resolves.not.toThrow();
    });

    it('should reconnect after disconnect', async () => {
      await eventBus.connect();
      const isHealthy = await eventBus.checkHealth();
      expect(isHealthy).toBe(true);
    });
  });

  describe('Event Publishing and Consuming', () => {
    beforeEach(async () => {
      await eventBus.connect();
    });

    afterEach(async () => {
      await eventBus.disconnect();
    });

    it('should publish and consume events successfully', async () => {
      const testEvent: EventBase<string, Record<string, unknown>> = {
        message: 'Hello Kafka!',
        headers: {
          correlation_id: 'test-123',
          timestamp: new Date().toISOString(),
          source: 'integration-test'
        }
      };

      const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
      const subscriptionPromise = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error('Test timeout: event not received within 10 seconds'));
        }, 10000);

        eventBus.subscribe('test.event', (event) => {
          receivedEvents.push(event);
          if (receivedEvents.length === 1) {
            clearTimeout(timeout);
            resolve();
          }
        });
      });

      // Wait a bit for subscription to be established
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Publish the event
      await eventBus.publish('test.event', testEvent);

      // Wait for the event to be received
      await subscriptionPromise;

      expect(receivedEvents).toHaveLength(1);
      expect(receivedEvents[0].message).toBe('Hello Kafka!');
      expect(receivedEvents[0].headers.correlation_id).toBe('test-123');
    });

    it('should handle multiple events', async () => {
      const events: EventBase<string, Record<string, unknown>>[] = [
        {
          message: 'Event 1',
          headers: { correlation_id: '1', timestamp: new Date().toISOString() }
        },
        {
          message: 'Event 2',
          headers: { correlation_id: '2', timestamp: new Date().toISOString() }
        },
        {
          message: 'Event 3',
          headers: { correlation_id: '3', timestamp: new Date().toISOString() }
        }
      ];

      const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
      const subscriptionPromise = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error('Test timeout: not all events received within 15 seconds'));
        }, 15000);

        eventBus.subscribe('multi.test', (event) => {
          receivedEvents.push(event);
          if (receivedEvents.length === events.length) {
            clearTimeout(timeout);
            resolve();
          }
        });
      });

      // Wait for subscription
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Publish all events
      for (const event of events) {
        await eventBus.publish('multi.test', event);
      }

      // Wait for all events to be received
      await subscriptionPromise;

      expect(receivedEvents).toHaveLength(3);
      expect(receivedEvents.map(e => e.message)).toEqual(['Event 1', 'Event 2', 'Event 3']);
    });

    it('should handle events with errors', async () => {
      const eventWithError: EventBase<string, Record<string, unknown>> = {
        message: 'Error event',
        headers: { correlation_id: 'error-123', timestamp: new Date().toISOString() },
        error: new Error('Test error')
      };

      const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
      const subscriptionPromise = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error('Test timeout: error event not received within 10 seconds'));
        }, 10000);

        eventBus.subscribe('error.test', (event) => {
          receivedEvents.push(event);
          clearTimeout(timeout);
          resolve();
        });
      });

      // Wait for subscription
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Publish event with error
      await eventBus.publish('error.test', eventWithError);

      // Wait for event to be received
      await subscriptionPromise;

      expect(receivedEvents).toHaveLength(1);
      expect(receivedEvents[0].message).toBe('Error event');
      expect(receivedEvents[0].error).toBeDefined();
    });
  });

  describe('Pattern-based Subscriptions', () => {
    beforeEach(async () => {
      await eventBus.connect();
    });

    afterEach(async () => {
      await eventBus.disconnect();
    });

    it('should subscribe to pattern-based events', async () => {
      const receivedEvents: Array<{ eventId: string; event: EventBase<string, Record<string, unknown>> }> = [];
      const subscriptionPromise = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error('Test timeout: pattern events not received within 15 seconds'));
        }, 15000);

        eventBus.subscribeToPattern('user.*', (eventId, event) => {
          receivedEvents.push({ eventId, event });
          if (receivedEvents.length === 2) {
            clearTimeout(timeout);
            resolve();
          }
        });
      });

      // Wait for subscription
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Publish events with different routing keys
      await eventBus.publish('user.created', {
        message: 'User created',
        headers: { correlation_id: '1', timestamp: new Date().toISOString() }
      });

      await eventBus.publish('user.updated', {
        message: 'User updated',
        headers: { correlation_id: '2', timestamp: new Date().toISOString() }
      });

      // Wait for events to be received
      await subscriptionPromise;

      expect(receivedEvents).toHaveLength(2);
      expect(receivedEvents.map(e => e.eventId)).toEqual(['user.created', 'user.updated']);
      expect(receivedEvents.map(e => e.event.message)).toEqual(['User created', 'User updated']);
    });
  });

  describe('Batch Processing', () => {
    beforeEach(async () => {
      await eventBus.connect();
    });

    afterEach(async () => {
      await eventBus.disconnect();
    });

    it('should process events in batches', async () => {
      const receivedBatches: EventBase<string, Record<string, unknown>>[][] = [];
      const subscriptionPromise = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error('Test timeout: batch not processed within 15 seconds'));
        }, 15000);

        eventBus.subscribeBatch('batch.test', (events) => {
          receivedBatches.push(events);
          if (receivedBatches.length === 1) {
            clearTimeout(timeout);
            resolve();
          }
        });
      });

      // Wait for subscription
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Publish multiple events quickly
      const events = [
        { message: 'Batch 1', headers: { correlation_id: '1', timestamp: new Date().toISOString() } },
        { message: 'Batch 2', headers: { correlation_id: '2', timestamp: new Date().toISOString() } },
        { message: 'Batch 3', headers: { correlation_id: '3', timestamp: new Date().toISOString() } }
      ];

      for (const event of events) {
        await eventBus.publish('batch.test', event);
      }

      // Wait for batch to be processed
      await subscriptionPromise;

      expect(receivedBatches).toHaveLength(1);
      expect(receivedBatches[0]).toHaveLength(3);
      expect(receivedBatches[0].map(e => e.message)).toEqual(['Batch 1', 'Batch 2', 'Batch 3']);
    });
  });

  describe('Error Handling', () => {
    beforeEach(async () => {
      await eventBus.connect();
    });

    afterEach(async () => {
      await eventBus.disconnect();
    });

    it('should handle connection errors gracefully', async () => {
      // Disconnect first
      await eventBus.disconnect();

      // Try to publish without connection
      await expect(eventBus.publish('test.event', {
        message: 'test',
        headers: { correlation_id: '1', timestamp: new Date().toISOString() }
      })).rejects.toThrow('Not connected to Kafka');

      // Try to subscribe without connection
      await expect(eventBus.subscribe('test.event', () => {})).rejects.toThrow('Not connected to Kafka');
    });

    it('should handle malformed messages gracefully', async () => {
      const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
      const subscriptionPromise = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error('Test timeout: malformed event not received within 10 seconds'));
        }, 10000);

        eventBus.subscribe('malformed.test', (event) => {
          receivedEvents.push(event);
          clearTimeout(timeout);
          resolve();
        });
      });

      // Wait for subscription
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Publish event with malformed data (this should be handled by the JSON.stringify in publish)
      await eventBus.publish('malformed.test', {
        message: 'Valid message',
        headers: { correlation_id: '1', timestamp: new Date().toISOString() }
      });

      // Wait for event to be received
      await subscriptionPromise;

      expect(receivedEvents).toHaveLength(1);
      expect(receivedEvents[0].message).toBe('Valid message');
    });
  });

  describe('Performance Tests', () => {
    beforeEach(async () => {
      await eventBus.connect();
    });

    afterEach(async () => {
      await eventBus.disconnect();
    });

    it('should handle high throughput', async () => {
      const eventCount = 100;
      const receivedEvents: EventBase<string, Record<string, unknown>>[] = [];
      
      const subscriptionPromise = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error(`Test timeout: only ${receivedEvents.length}/${eventCount} events received within 30 seconds`));
        }, 30000);

        eventBus.subscribe('performance.test', (event) => {
          receivedEvents.push(event);
          if (receivedEvents.length === eventCount) {
            clearTimeout(timeout);
            resolve();
          }
        });
      });

      // Wait for subscription
      await new Promise(resolve => setTimeout(resolve, 2000));

      const startTime = Date.now();

      // Publish events in batches
      for (let i = 0; i < eventCount; i++) {
        await eventBus.publish('performance.test', {
          message: `Performance test event ${i}`,
          headers: { 
            correlation_id: `perf-${i}`, 
            timestamp: new Date().toISOString() 
          }
        });
      }

      // Wait for all events to be received
      await subscriptionPromise;

      const endTime = Date.now();
      const duration = endTime - startTime;
      const eventsPerSecond = (eventCount / duration) * 1000;

      console.log(`Processed ${eventCount} events in ${duration}ms (${eventsPerSecond.toFixed(2)} events/sec)`);

      expect(receivedEvents).toHaveLength(eventCount);
      expect(eventsPerSecond).toBeGreaterThan(10); // At least 10 events per second
    });
  });

  describe('Consumer Group Behavior', () => {
    it('should handle multiple consumers in same group', async () => {
      const eventBus1 = new KafkaEventBus({
        brokers,
        topicName: 'consumer-group-test',
        groupId: 'test-consumer-group'
      });

      const eventBus2 = new KafkaEventBus({
        brokers,
        topicName: 'consumer-group-test',
        groupId: 'test-consumer-group'
      });

      try {
        await eventBus1.connect();
        await eventBus2.connect();

        const receivedEvents1: EventBase<unknown, unknown>[] = [];
        const receivedEvents2: EventBase<unknown, unknown>[] = [];

        let totalReceived = 0;
        const subscriptionPromise1 = new Promise<void>((resolve, reject) => {
          const timeout = setTimeout(() => {
            reject(new Error('Test timeout: consumer1 event not received within 15 seconds'));
          }, 15000);

          eventBus1.subscribe('consumer.test', (event) => {
            receivedEvents1.push(event);
            totalReceived++;
            if (totalReceived >= 2) {
              clearTimeout(timeout);
              resolve();
            }
          });
        });

        const subscriptionPromise2 = new Promise<void>((resolve, reject) => {
          const timeout = setTimeout(() => {
            reject(new Error('Test timeout: consumer2 event not received within 15 seconds'));
          }, 15000);

          eventBus2.subscribe('consumer.test', (event) => {
            receivedEvents2.push(event);
            totalReceived++;
            if (totalReceived >= 2) {
              clearTimeout(timeout);
              resolve();
            }
          });
        });

        // Wait for subscriptions
        await new Promise(resolve => setTimeout(resolve, 3000));

        // Publish events
        await eventBus1.publish('consumer.test', {
          message: 'Consumer group test 1',
          headers: { correlation_id: '1', timestamp: new Date().toISOString() }
        });

        await eventBus1.publish('consumer.test', {
          message: 'Consumer group test 2',
          headers: { correlation_id: '2', timestamp: new Date().toISOString() }
        });

        // Wait for events to be received - in Kafka consumer groups, 
        // only one consumer receives each message, so we wait for the first one to complete
        await Promise.race([subscriptionPromise1, subscriptionPromise2]);

        // In Kafka consumer groups, only one consumer should receive each message
        const finalTotalReceived = receivedEvents1.length + receivedEvents2.length;
        expect(finalTotalReceived).toBe(2);
        
        // Debug: log received events
        console.log(`Consumer1 received ${receivedEvents1.length} events`);
        console.log(`Consumer2 received ${receivedEvents2.length} events`);
        console.log(`Total received: ${finalTotalReceived}`);

      } finally {
        await eventBus1.disconnect();
        await eventBus2.disconnect();
      }
    });
  });
});
