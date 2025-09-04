import { KafkaEventBus } from '../kafka-event-bus';
import { EventBase } from '@soapjs/soap';

// Mock kafkajs
jest.mock('kafkajs', () => ({
  Kafka: jest.fn().mockImplementation(() => ({
    producer: jest.fn(),
    consumer: jest.fn(),
    admin: jest.fn(),
  })),
}));

describe('KafkaEventBus', () => {
  let eventBus: KafkaEventBus<string, Record<string, unknown>, string>;
  let mockKafka: any;
  let mockProducer: any;
  let mockConsumer: any;
  let mockAdmin: any;

  beforeEach(() => {
    mockProducer = {
      connect: jest.fn(),
      disconnect: jest.fn(),
      send: jest.fn(),
    };

    mockConsumer = {
      connect: jest.fn(),
      disconnect: jest.fn(),
      subscribe: jest.fn(),
      run: jest.fn(),
      stop: jest.fn(),
    };

    mockAdmin = {
      connect: jest.fn(),
      disconnect: jest.fn(),
      createTopics: jest.fn(),
      listTopics: jest.fn(),
    };

    mockKafka = {
      producer: jest.fn().mockReturnValue(mockProducer),
      consumer: jest.fn().mockReturnValue(mockConsumer),
      admin: jest.fn().mockReturnValue(mockAdmin),
    };

    const { Kafka } = require('kafkajs');
    Kafka.mockImplementation(() => mockKafka);

    eventBus = new KafkaEventBus({
      brokers: ['localhost:9092'],
      topicName: 'test-events',
      groupId: 'test-group'
    });
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('constructor', () => {
    it('should create KafkaEventBus with default options', () => {
      const defaultEventBus = new KafkaEventBus({
        brokers: ['localhost:9092']
      });

      expect(defaultEventBus).toBeDefined();
    });

    it('should throw error if brokers are not provided', () => {
      expect(() => {
        new KafkaEventBus({
          brokers: []
        });
      }).toThrow('Kafka brokers are required');
    });
  });

  describe('connect', () => {
    it('should connect to Kafka and set up producer, consumer, and topic', async () => {
      mockAdmin.createTopics.mockRejectedValue(new Error('Topic already exists')); // Simulate existing topic

      await eventBus.connect();

      expect(mockProducer.connect).toHaveBeenCalled();
      expect(mockConsumer.connect).toHaveBeenCalled();
      expect(mockAdmin.connect).toHaveBeenCalled();
      expect(mockAdmin.createTopics).toHaveBeenCalledWith({
        topics: [{
          topic: 'test-events',
          numPartitions: 3,
          replicationFactor: 1
        }]
      });
      expect(mockAdmin.disconnect).toHaveBeenCalled();
    });

    it('should create topic if it does not exist', async () => {
      mockAdmin.createTopics.mockResolvedValue(undefined);

      await eventBus.connect();

      expect(mockAdmin.createTopics).toHaveBeenCalled();
    });

    it('should throw error if connection fails', async () => {
      mockProducer.connect.mockRejectedValue(new Error('Connection failed'));

      await expect(eventBus.connect()).rejects.toThrow('Failed to connect to Kafka');
    });
  });

  describe('disconnect', () => {
    it('should disconnect consumer and producer', async () => {
      await eventBus.connect();
      await eventBus.disconnect();

      expect(mockConsumer.disconnect).toHaveBeenCalled();
      expect(mockProducer.disconnect).toHaveBeenCalled();
    });

    it('should handle disconnect when not connected', async () => {
      await expect(eventBus.disconnect()).resolves.not.toThrow();
    });
  });

  describe('checkHealth', () => {
    it('should return true when connection is healthy', async () => {
      await eventBus.connect();
      mockAdmin.listTopics.mockResolvedValue(['test-events']);

      const isHealthy = await eventBus.checkHealth();

      expect(isHealthy).toBe(true);
      expect(mockAdmin.connect).toHaveBeenCalled();
      expect(mockAdmin.listTopics).toHaveBeenCalled();
      expect(mockAdmin.disconnect).toHaveBeenCalled();
    });

    it('should return false when not connected', async () => {
      const isHealthy = await eventBus.checkHealth();
      expect(isHealthy).toBe(false);
    });

    it('should return false when health check fails', async () => {
      await eventBus.connect();
      mockAdmin.listTopics.mockRejectedValue(new Error('Health check failed'));

      const isHealthy = await eventBus.checkHealth();
      expect(isHealthy).toBe(false);
    });
  });

  describe('publish', () => {
    it('should publish event to Kafka', async () => {
      await eventBus.connect();
      mockProducer.send.mockResolvedValue(undefined);

      const eventData: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { correlation_id: '123', timestamp: '2023-01-01' }
      };

      await eventBus.publish('test.event', eventData);

      expect(mockProducer.send).toHaveBeenCalledWith({
        topic: 'test-events',
        messages: [{
          key: 'test.event',
          value: JSON.stringify({
            message: eventData.message,
            headers: eventData.headers,
            error: eventData.error
          }),
          headers: {
            correlation_id: '123',
            timestamp: '2023-01-01'
          },
          timestamp: expect.any(String)
        }]
      });
    });

    it('should throw error if not connected', async () => {
      const eventData: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: {}
      };

      await expect(eventBus.publish('test.event', eventData))
        .rejects.toThrow('Not connected to Kafka');
    });

    it('should handle headers conversion', async () => {
      await eventBus.connect();
      mockProducer.send.mockResolvedValue(undefined);

      const eventData: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { 
          correlation_id: '123',
          numeric_value: 456,
          boolean_value: true
        }
      };

      await eventBus.publish('test.event', eventData);

      expect(mockProducer.send).toHaveBeenCalledWith({
        topic: 'test-events',
        messages: [{
          key: 'test.event',
          value: expect.any(String),
          headers: {
            correlation_id: '123',
            numeric_value: '456',
            boolean_value: 'true'
          },
          timestamp: expect.any(String)
        }]
      });
    });
  });

  describe('subscribe', () => {
    it('should subscribe to event and set up consumer', async () => {
      await eventBus.connect();
      mockConsumer.subscribe.mockResolvedValue(undefined);
      mockConsumer.run.mockResolvedValue(undefined);

      const handler = jest.fn();
      await eventBus.subscribe('test.event', handler);

      expect(mockConsumer.subscribe).toHaveBeenCalledWith({ 
        topic: 'test-events', 
        fromBeginning: false 
      });
      expect(mockConsumer.run).toHaveBeenCalledWith({
        eachMessage: expect.any(Function)
      });
    });

    it('should throw error if not connected', async () => {
      const handler = jest.fn();
      await expect(eventBus.subscribe('test.event', handler))
        .rejects.toThrow('Not connected to Kafka');
    });

    it('should call handler when message matches event key', async () => {
      await eventBus.connect();
      mockConsumer.subscribe.mockResolvedValue(undefined);
      
      let messageHandler: any;
      mockConsumer.run.mockImplementation(({ eachMessage }) => {
        messageHandler = eachMessage;
        return Promise.resolve();
      });

      const handler = jest.fn();
      await eventBus.subscribe('test.event', handler);

      // Simulate receiving a message
      const message = {
        topic: 'test-events',
        partition: 0,
        message: {
          key: Buffer.from('test.event'),
          value: Buffer.from(JSON.stringify({
            message: 'test message',
            headers: { correlation_id: '123' }
          }))
        }
      };

      await messageHandler(message);

      expect(handler).toHaveBeenCalledWith({
        message: 'test message',
        headers: { correlation_id: '123' }
      });
    });

    it('should not call handler when message key does not match', async () => {
      await eventBus.connect();
      mockConsumer.subscribe.mockResolvedValue(undefined);
      
      let messageHandler: any;
      mockConsumer.run.mockImplementation(({ eachMessage }) => {
        messageHandler = eachMessage;
        return Promise.resolve();
      });

      const handler = jest.fn();
      await eventBus.subscribe('test.event', handler);

      // Simulate receiving a message with different key
      const message = {
        topic: 'test-events',
        partition: 0,
        message: {
          key: Buffer.from('other.event'),
          value: Buffer.from(JSON.stringify({
            message: 'test message',
            headers: { correlation_id: '123' }
          }))
        }
      };

      await messageHandler(message);

      expect(handler).not.toHaveBeenCalled();
    });
  });

  describe('setRetryPolicy', () => {
    it('should set retry policy', () => {
      const backoffOptions = {
        type: 'exponential' as const,
        multiplier: 2,
        maxDelay: 30000,
        jitter: true
      };

      eventBus.setRetryPolicy(5, 2000, backoffOptions);

      // We can't directly test the private property, but we can test that it doesn't throw
      expect(() => eventBus.setRetryPolicy(5, 2000, backoffOptions)).not.toThrow();
    });
  });

  describe('unsubscribe', () => {
    it('should unsubscribe from event', async () => {
      await eventBus.connect();
      mockConsumer.subscribe.mockResolvedValue(undefined);
      mockConsumer.run.mockResolvedValue(undefined);
      mockConsumer.stop.mockResolvedValue(undefined);

      // First subscribe to create a subscription
      await eventBus.subscribe('test.event', jest.fn());
      
      // Test that unsubscribe doesn't throw when called with valid subscription
      await expect(eventBus.unsubscribe('test-subscription')).resolves.not.toThrow();
    });

    it('should handle unsubscribe when not connected', async () => {
      await expect(eventBus.unsubscribe('test-subscription')).resolves.not.toThrow();
    });
  });

  describe('acknowledge', () => {
    it('should acknowledge message', async () => {
      await eventBus.connect();
      
      // acknowledge is a no-op in our implementation, but we test it doesn't throw
      await expect(eventBus.acknowledge('test-message-id')).resolves.not.toThrow();
    });
  });

  describe('reject', () => {
    it('should reject message', async () => {
      await eventBus.connect();
      
      // reject is a no-op in our implementation, but we test it doesn't throw
      await expect(eventBus.reject('test-message-id', false)).resolves.not.toThrow();
      await expect(eventBus.reject('test-message-id', true)).resolves.not.toThrow();
    });
  });

  describe('subscribeToPattern', () => {
    it('should subscribe to pattern', async () => {
      await eventBus.connect();
      mockConsumer.subscribe.mockResolvedValue(undefined);
      mockConsumer.run.mockResolvedValue(undefined);

      const handler = jest.fn();
      const subscriptionId = await eventBus.subscribeToPattern('test.*', handler);

      expect(mockConsumer.subscribe).toHaveBeenCalledWith({ 
        topic: 'test-events', 
        fromBeginning: false 
      });
      expect(mockConsumer.run).toHaveBeenCalledWith({
        eachMessage: expect.any(Function)
      });
      expect(subscriptionId).toBeDefined();
    });

    it('should throw error if not connected', async () => {
      const handler = jest.fn();
      await expect(eventBus.subscribeToPattern('test.*', handler))
        .rejects.toThrow('Not connected to Kafka');
    });

    it('should call handler when message key matches pattern', async () => {
      await eventBus.connect();
      mockConsumer.subscribe.mockResolvedValue(undefined);
      
      let messageHandler: any;
      mockConsumer.run.mockImplementation(({ eachMessage }) => {
        messageHandler = eachMessage;
        return Promise.resolve();
      });

      const handler = jest.fn();
      await eventBus.subscribeToPattern('test.*', handler);

      // Simulate receiving a message that matches pattern
      const message = {
        topic: 'test-events',
        partition: 0,
        message: {
          key: Buffer.from('test.created'),
          value: Buffer.from(JSON.stringify({
            message: 'test message',
            headers: { correlation_id: '123' }
          }))
        }
      };

      await messageHandler(message);

      expect(handler).toHaveBeenCalledWith('test.created', {
        message: 'test message',
        headers: { correlation_id: '123' }
      });
    });
  });

  describe('subscribeBatch', () => {
    it('should subscribe to batch events', async () => {
      await eventBus.connect();
      mockConsumer.subscribe.mockResolvedValue(undefined);
      mockConsumer.run.mockResolvedValue(undefined);

      const handler = jest.fn();
      const subscriptionId = await eventBus.subscribeBatch('test.event', handler);

      expect(mockConsumer.subscribe).toHaveBeenCalledWith({ 
        topic: 'test-events', 
        fromBeginning: false 
      });
      expect(mockConsumer.run).toHaveBeenCalledWith({
        eachMessage: expect.any(Function)
      });
      expect(subscriptionId).toBeDefined();
    });

    it('should throw error if not connected', async () => {
      const handler = jest.fn();
      await expect(eventBus.subscribeBatch('test.event', handler))
        .rejects.toThrow('Not connected to Kafka');
    });

    it('should process messages in batches', async () => {
      await eventBus.connect();
      mockConsumer.subscribe.mockResolvedValue(undefined);
      
      let messageHandler: any;
      mockConsumer.run.mockImplementation(({ eachMessage }) => {
        messageHandler = eachMessage;
        return Promise.resolve();
      });

      const handler = jest.fn();
      await eventBus.subscribeBatch('test.event', handler);

      // Simulate receiving multiple messages
      const message1 = {
        topic: 'test-events',
        partition: 0,
        message: {
          key: Buffer.from('test.event'),
          value: Buffer.from(JSON.stringify({
            message: 'test message 1',
            headers: { correlation_id: '123' }
          }))
        }
      };

      const message2 = {
        topic: 'test-events',
        partition: 0,
        message: {
          key: Buffer.from('test.event'),
          value: Buffer.from(JSON.stringify({
            message: 'test message 2',
            headers: { correlation_id: '124' }
          }))
        }
      };

      await messageHandler(message1);
      await messageHandler(message2);

      // Wait for batch processing
      await new Promise(resolve => setTimeout(resolve, 1100));

      expect(handler).toHaveBeenCalledWith([
        {
          message: 'test message 1',
          headers: { correlation_id: '123' }
        },
        {
          message: 'test message 2',
          headers: { correlation_id: '124' }
        }
      ]);
    });
  });

  describe('publish error handling', () => {
    it('should throw error when publishing fails', async () => {
      await eventBus.connect();
      mockProducer.send.mockRejectedValue(new Error('Publish failed'));

      const eventData: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { correlation_id: '123' }
      };

      await expect(eventBus.publish('test.event', eventData))
        .rejects.toThrow('Failed to publish event');
    });
  });

  describe('subscribe error handling', () => {
    it('should handle JSON parsing errors in message handler', async () => {
      await eventBus.connect();
      mockConsumer.subscribe.mockResolvedValue(undefined);
      
      let messageHandler: any;
      mockConsumer.run.mockImplementation(({ eachMessage }) => {
        messageHandler = eachMessage;
        return Promise.resolve();
      });

      const handler = jest.fn();
      await eventBus.subscribe('test.event', handler);

      // Simulate malformed message
      const malformedMessage = {
        topic: 'test-events',
        partition: 0,
        message: {
          key: Buffer.from('test.event'),
          value: Buffer.from('invalid json')
        }
      };

      // Mock console.error to avoid noise in test output
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();

      await messageHandler(malformedMessage);

      expect(consoleSpy).toHaveBeenCalledWith('Error processing message:', expect.any(Error));
      expect(handler).not.toHaveBeenCalled();

      consoleSpy.mockRestore();
    });
  });

  describe('convertHeadersToKafkaHeaders', () => {
    it('should convert headers to Kafka format', async () => {
      await eventBus.connect();
      mockProducer.send.mockResolvedValue(undefined);

      const eventData: EventBase<string, Record<string, unknown>> = {
        message: 'test message',
        headers: { 
          string_value: 'test',
          number_value: 123,
          boolean_value: true,
          null_value: null,
          undefined_value: undefined
        }
      };

      await eventBus.publish('test.event', eventData);

      expect(mockProducer.send).toHaveBeenCalledWith({
        topic: 'test-events',
        messages: [{
          key: 'test.event',
          value: expect.any(String),
          headers: {
            string_value: 'test',
            number_value: '123',
            boolean_value: 'true',
            null_value: 'null',
            undefined_value: 'undefined'
          },
          timestamp: expect.any(String)
        }]
      });
    });
  });
});
