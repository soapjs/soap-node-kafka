import { Kafka, Producer, Consumer, EachMessagePayload, KafkaMessage } from 'kafkajs';
import { EventBus, BackoffOptions } from '@soapjs/soap';
import { EventBase } from '@soapjs/soap';

/**
 * Configuration options for KafkaEventBus
 */
export interface KafkaEventBusOptions {
  /** Kafka broker URLs (e.g., ['localhost:9092']) */
  brokers: string[];
  /** Client ID for Kafka client (default: 'soap-event-bus') */
  clientId?: string;
  /** Topic name for events (default: 'soap-events') */
  topicName?: string;
  /** Consumer group ID (default: 'soap-consumer-group') */
  groupId?: string;
  /** Retry policy for failed operations */
  retryPolicy?: {
    /** Maximum number of retry attempts */
    maxRetries: number;
    /** Delay between retries in milliseconds */
    delay: number;
    /** Backoff strategy configuration */
    backoff?: BackoffOptions;
  };
  /** Kafka client configuration */
  kafkaConfig?: {
    /** Connection timeout in milliseconds */
    connectionTimeout?: number;
    /** Request timeout in milliseconds */
    requestTimeout?: number;
    /** Retry configuration */
    retry?: {
      /** Initial retry time in milliseconds */
      initialRetryTime?: number;
      /** Maximum retry time in milliseconds */
      maxRetryTime?: number;
      /** Retry multiplier */
      multiplier?: number;
      /** Maximum number of retries */
      maxRetries?: number;
    };
  };
}

/**
 * Kafka implementation of the SoapJS EventBus interface.
 * 
 * Provides reliable event publishing, consuming, and processing capabilities
 * using Apache Kafka as the message broker backend.
 * 
 * @template MessageType - Type of the message payload
 * @template HeadersType - Type of the message headers
 * @template EventIdType - Type of the event identifier (default: string)
 * 
 * @example
 * ```typescript
 * const eventBus = new KafkaEventBus({
 *   brokers: ['localhost:9092'],
 *   topicName: 'my-events',
 *   groupId: 'my-service-group'
 * });
 * 
 * await eventBus.connect();
 * 
 * // Publish an event
 * await eventBus.publish('user.created', {
 *   message: { userId: '123', name: 'John Doe' },
 *   headers: { correlation_id: 'corr-123', timestamp: new Date().toISOString() }
 * });
 * 
 * // Subscribe to events
 * await eventBus.subscribe('user.created', (eventData) => {
 *   console.log('User created:', eventData);
 * });
 * ```
 */
export class KafkaEventBus<MessageType, HeadersType, EventIdType = string> 
  implements EventBus<MessageType, HeadersType, EventIdType> {
  
  private kafka: Kafka;
  private producer: Producer | null = null;
  private consumer: Consumer | null = null;
  private subscriptions: Map<string, string> = new Map(); // subscriptionId -> eventKey
  private eventHandlers: Map<string, (data: EventBase<MessageType, HeadersType>, eventKey?: string) => void> = new Map(); // eventKey -> handler
  private retryPolicy: {
    maxRetries: number;
    delay: number;
    backoff?: BackoffOptions;
  };
  private isConnected: boolean = false;

  /**
   * Creates a new KafkaEventBus instance.
   * 
   * @param options - Configuration options for the event bus
   * @throws {Error} If required options are missing or invalid
   * 
   * @example
   * ```typescript
   * const eventBus = new KafkaEventBus({
   *   brokers: ['localhost:9092'],
   *   topicName: 'my-events',
   *   groupId: 'my-service-group',
   *   retryPolicy: {
   *     maxRetries: 5,
   *     delay: 2000,
   *     backoff: {
   *       type: 'exponential',
   *       multiplier: 2,
   *       maxDelay: 30000,
   *       jitter: true
   *     }
   *   }
   * });
   * ```
   */
  constructor(private options: KafkaEventBusOptions) {
    if (!options.brokers || options.brokers.length === 0) {
      throw new Error('Kafka brokers are required');
    }

    this.retryPolicy = options.retryPolicy || {
      maxRetries: 3,
      delay: 1000,
      backoff: {
        type: 'exponential',
        multiplier: 2,
        maxDelay: 30000,
        jitter: true
      }
    };

    this.kafka = new Kafka({
      clientId: options.clientId || 'soap-event-bus',
      brokers: options.brokers,
      connectionTimeout: options.kafkaConfig?.connectionTimeout || 3000,
      requestTimeout: options.kafkaConfig?.requestTimeout || 30000,
      retry: {
        initialRetryTime: options.kafkaConfig?.retry?.initialRetryTime || 100,
        maxRetryTime: options.kafkaConfig?.retry?.maxRetryTime || 30000,
        multiplier: options.kafkaConfig?.retry?.multiplier || 2,
        retries: options.kafkaConfig?.retry?.maxRetries || 5,
      }
    });
  }

  /**
   * Establishes connection to Kafka and sets up producer and consumer.
   * 
   * This method creates a producer and consumer, and ensures the topic exists.
   * 
   * @returns Promise that resolves when connection is established
   * @throws {Error} If connection fails or topic creation fails
   * 
   * @example
   * ```typescript
   * try {
   *   await eventBus.connect();
   *   console.log('Connected to Kafka');
   * } catch (error) {
   *   console.error('Connection failed:', error);
   * }
   * ```
   */
  async connect(): Promise<void> {
    try {
      // Create producer
      this.producer = this.kafka.producer();
      await this.producer.connect();

      // Create consumer
      this.consumer = this.kafka.consumer({ 
        groupId: this.options.groupId || 'soap-consumer-group' 
      });
      await this.consumer.connect();

      // Ensure topic exists
      const topicName = this.options.topicName || 'soap-events';
      const admin = this.kafka.admin();
      await admin.connect();
      
      try {
        await admin.createTopics({
          topics: [{
            topic: topicName,
            numPartitions: 3,
            replicationFactor: 1
          }]
        });
      } catch (error) {
        // Topic might already exist, which is fine
        console.log('Topic creation skipped (might already exist):', error);
      }
      
      await admin.disconnect();
      this.isConnected = true;
      
    } catch (error) {
      throw new Error(`Failed to connect to Kafka: ${error}`);
    }
  }

  /**
   * Closes the connection to Kafka and cleans up resources.
   * 
   * This method gracefully disconnects the producer and consumer.
   * 
   * @returns Promise that resolves when disconnection is complete
   * @throws {Error} If disconnection fails
   * 
   * @example
   * ```typescript
   * try {
   *   await eventBus.disconnect();
   *   console.log('Disconnected from Kafka');
   * } catch (error) {
   *   console.error('Disconnection failed:', error);
   * }
   * ```
   */
  async disconnect(): Promise<void> {
    try {
      if (this.consumer) {
        await this.consumer.disconnect();
        this.consumer = null;
      }
      if (this.producer) {
        await this.producer.disconnect();
        this.producer = null;
      }
      this.isConnected = false;
      this.subscriptions.clear();
      this.eventHandlers.clear();
    } catch (error) {
      throw new Error(`Failed to disconnect from Kafka: ${error}`);
    }
  }

  /**
   * Checks if the Kafka connection is healthy and responsive.
   * 
   * This method performs a lightweight health check by attempting to
   * get metadata from the Kafka cluster.
   * 
   * @returns Promise that resolves to true if connection is healthy, false otherwise
   * 
   * @example
   * ```typescript
   * const isHealthy = await eventBus.checkHealth();
   * if (!isHealthy) {
   *   console.log('Connection is unhealthy, attempting to reconnect...');
   *   await eventBus.connect();
   * }
   * ```
   */
  async checkHealth(): Promise<boolean> {
    try {
      if (!this.isConnected || !this.producer) {
        return false;
      }
      
      // Try to get metadata to check if the connection is alive
      const admin = this.kafka.admin();
      await admin.connect();
      await admin.listTopics();
      await admin.disconnect();
      return true;
    } catch (error) {
      return false;
    }
  }

  /**
   * Publishes an event to Kafka.
   * 
   * This method serializes the event data and publishes it to the configured
   * topic with the event ID as the message key.
   * 
   * @param event - The event identifier (used as message key)
   * @param eventData - The event data containing message, headers, and optional error
   * @returns Promise that resolves when the event is published
   * @throws {Error} If not connected to Kafka or publishing fails
   * 
   * @example
   * ```typescript
   * await eventBus.publish('user.created', {
   *   message: { userId: '123', name: 'John Doe' },
   *   headers: { 
   *     correlation_id: 'corr-123', 
   *     timestamp: new Date().toISOString(),
   *     source: 'user-service'
   *   }
   * });
   * ```
   */
  async publish(event: EventIdType, eventData: EventBase<MessageType, HeadersType>): Promise<void> {
    if (!this.producer) {
      throw new Error('Not connected to Kafka');
    }

    try {
      const topicName = this.options.topicName || 'soap-events';
      const key = String(event);
      
      const message = {
        key,
        value: JSON.stringify({
          message: eventData.message,
          headers: eventData.headers,
          error: eventData.error
        }),
        headers: this.convertHeadersToKafkaHeaders(eventData.headers),
        timestamp: Date.now().toString()
      };

      await this.producer.send({
        topic: topicName,
        messages: [message]
      });
    } catch (error) {
      throw new Error(`Failed to publish event: ${error}`);
    }
  }

  /**
   * Subscribes to events with the specified event ID.
   * 
   * This method subscribes to the topic and filters messages by event ID.
   * 
   * @param event - The event identifier to subscribe to
   * @param handler - Function to handle incoming events
   * @returns Promise that resolves when subscription is set up
   * @throws {Error} If not connected to Kafka or subscription setup fails
   * 
   * @example
   * ```typescript
   * await eventBus.subscribe('user.created', (eventData) => {
   *   console.log('Received user created event:', eventData.message);
   *   console.log('Headers:', eventData.headers);
   * });
   * ```
   */
  async subscribe(event: EventIdType, handler: (data: EventBase<MessageType, HeadersType>) => void): Promise<void> {
    if (!this.consumer) {
      throw new Error('Not connected to Kafka');
    }

    try {
      const topicName = this.options.topicName || 'soap-events';
      const eventKey = String(event);

      // Store subscription for cleanup
      const subscriptionId = this.generateSubscriptionId();
      this.subscriptions.set(subscriptionId, eventKey);
      this.eventHandlers.set(eventKey, handler);

      // If this is the first subscription, start the consumer
      if (this.subscriptions.size === 1) {
        await this.consumer.subscribe({ topic: topicName, fromBeginning: false });

        await this.consumer.run({
          eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
            try {
              const eventKey = message.key?.toString();
              if (eventKey) {
                const eventData: EventBase<MessageType, HeadersType> = JSON.parse(message.value?.toString() || '{}');
                
                // Check all handler types
                for (const [handlerKey, handlerFn] of this.eventHandlers.entries()) {
                  if (handlerKey.startsWith('batch:')) {
                    const batchEventKey = handlerKey.substring(6); // Remove 'batch:' prefix
                    if (batchEventKey === eventKey) {
                      handlerFn(eventData, eventKey);
                    }
                  } else if (handlerKey.startsWith('pattern:')) {
                    const pattern = handlerKey.substring(8); // Remove 'pattern:' prefix
                    const regex = new RegExp(pattern.replace(/\*/g, '.*'));
                    if (regex.test(eventKey)) {
                      handlerFn(eventData, eventKey);
                    }
                  } else if (handlerKey === eventKey) {
                    // Regular event handler
                    handlerFn(eventData, eventKey);
                  }
                }
              }
            } catch (error) {
              console.error('Error processing message:', error);
            }
          }
        });
      }
      
    } catch (error) {
      throw new Error(`Failed to subscribe to event: ${error}`);
    }
  }

  /**
   * Unsubscribes from an event by stopping the consumer.
   * 
   * This method stops the consumer and removes the subscription.
   * 
   * @param subscriptionId - The subscription ID to unsubscribe from
   * @returns Promise that resolves when unsubscription is complete
   * @throws {Error} If unsubscription fails
   * 
   * @example
   * ```typescript
   * const subscriptionId = 'sub-123';
   * await eventBus.unsubscribe(subscriptionId);
   * ```
   */
  async unsubscribe(subscriptionId: string): Promise<void> {
    const subscriptionKey = this.subscriptions.get(subscriptionId);
    if (!subscriptionKey || !this.consumer) {
      return;
    }

    try {
      // Remove the handler based on subscription type
      this.eventHandlers.delete(subscriptionKey);
      this.subscriptions.delete(subscriptionId);
      
      // If no more subscriptions, stop the consumer
      if (this.subscriptions.size === 0) {
        await this.consumer.stop();
      }
    } catch (error) {
      throw new Error(`Failed to unsubscribe: ${error}`);
    }
  }

  /**
   * Acknowledges a message (no-op in this implementation).
   * 
   * In Kafka, message acknowledgment is handled automatically by the consumer.
   * This method is kept for interface compatibility with the EventBus interface.
   * 
   * @param messageId - The message ID to acknowledge
   * @returns Promise that resolves immediately
   * 
   * @example
   * ```typescript
   * await eventBus.acknowledge('msg-123');
   * ```
   */
  async acknowledge(messageId: string): Promise<void> {
    // In Kafka, acknowledgment is handled automatically by the consumer
    // This method is kept for interface compatibility
  }

  /**
   * Rejects a message (no-op in this implementation).
   * 
   * In Kafka, message rejection is handled by the consumer group.
   * This method is kept for interface compatibility with the EventBus interface.
   * 
   * @param messageId - The message ID to reject
   * @param requeue - Whether to requeue the message (ignored in this implementation)
   * @returns Promise that resolves immediately
   * 
   * @example
   * ```typescript
   * await eventBus.reject('msg-123', false);
   * ```
   */
  async reject(messageId: string, requeue: boolean = false): Promise<void> {
    // In Kafka, rejection is handled by the consumer group
    // This method is kept for interface compatibility
  }

  /**
   * Sets the retry policy for failed operations.
   * 
   * This method configures the retry behavior for operations that may fail,
   * such as connection attempts or message publishing.
   * 
   * @param retries - Maximum number of retry attempts
   * @param delay - Base delay between retries in milliseconds
   * @param backoff - Optional backoff strategy configuration
   * 
   * @example
   * ```typescript
   * eventBus.setRetryPolicy(5, 2000, {
   *   type: 'exponential',
   *   multiplier: 2,
   *   maxDelay: 30000,
   *   jitter: true
   * });
   * ```
   */
  setRetryPolicy(retries: number, delay: number, backoff?: BackoffOptions): void {
    this.retryPolicy = {
      maxRetries: retries,
      delay,
      backoff
    };
  }

  /**
   * Subscribes to events matching a pattern.
   * 
   * This method subscribes to the topic and filters messages by pattern matching.
   * 
   * @param pattern - The pattern to match (e.g., 'user.*', '*.created')
   * @param handler - Function to handle matching events
   * @returns Promise that resolves to the subscription ID
   * @throws {Error} If not connected to Kafka or subscription setup fails
   * 
   * @example
   * ```typescript
   * const subscriptionId = await eventBus.subscribeToPattern('user.*', (eventId, eventData) => {
   *   console.log(`Received ${eventId}:`, eventData.message);
   * });
   * ```
   */
  async subscribeToPattern(pattern: string, handler: (eventId: EventIdType, event: EventBase<MessageType, HeadersType>) => void): Promise<string> {
    if (!this.consumer) {
      throw new Error('Not connected to Kafka');
    }

    try {
      const topicName = this.options.topicName || 'soap-events';
      const regex = new RegExp(pattern.replace(/\*/g, '.*'));
      const subscriptionId = this.generateSubscriptionId();

      // Store pattern handler for client-side filtering
      this.eventHandlers.set(`pattern:${pattern}`, (eventData: EventBase<MessageType, HeadersType>, eventKey?: string) => {
        // This will be called from the main consumer's eachMessage handler
        if (eventKey && regex.test(eventKey)) {
          handler(eventKey as EventIdType, eventData);
        }
      });

      // If this is the first subscription, start the consumer
      if (this.subscriptions.size === 0) {
        await this.consumer.subscribe({ topic: topicName, fromBeginning: false });

        await this.consumer.run({
          eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
            try {
              const eventKey = message.key?.toString();
              if (eventKey) {
                const eventData: EventBase<MessageType, HeadersType> = JSON.parse(message.value?.toString() || '{}');
                
                // Check pattern handlers
                for (const [handlerKey, handlerFn] of this.eventHandlers.entries()) {
                  if (handlerKey.startsWith('pattern:')) {
                    const pattern = handlerKey.substring(8); // Remove 'pattern:' prefix
                    const regex = new RegExp(pattern.replace(/\*/g, '.*'));
                    if (regex.test(eventKey)) {
                      handlerFn(eventData, eventKey);
                    }
                  } else if (handlerKey === eventKey) {
                    // Regular event handler
                    handlerFn(eventData, eventKey);
                  }
                }
              }
            } catch (error) {
              console.error('Error processing message:', error);
            }
          }
        });
      }

      this.subscriptions.set(subscriptionId, `pattern:${pattern}`);
      return subscriptionId;
      
    } catch (error) {
      throw new Error(`Failed to subscribe to pattern: ${error}`);
    }
  }

  /**
   * Subscribes to events for batch processing.
   * 
   * This method subscribes to the topic and processes events in batches.
   * 
   * @param event - The event identifier to subscribe to
   * @param handler - Function to handle batches of events
   * @returns Promise that resolves to the subscription ID
   * @throws {Error} If not connected to Kafka or subscription setup fails
   * 
   * @example
   * ```typescript
   * const subscriptionId = await eventBus.subscribeBatch('user.events', (events) => {
   *   console.log(`Processing ${events.length} events in batch`);
   *   events.forEach(event => {
   *     // Process each event in the batch
   *     console.log('Event:', event.message);
   *   });
   * });
   * ```
   */
  async subscribeBatch(event: EventIdType, handler: (events: EventBase<MessageType, HeadersType>[]) => void): Promise<string> {
    if (!this.consumer) {
      throw new Error('Not connected to Kafka');
    }

    try {
      const topicName = this.options.topicName || 'soap-events';
      const eventKey = String(event);
      const subscriptionId = this.generateSubscriptionId();

      // Create batch processing state
      const batch: EventBase<MessageType, HeadersType>[] = [];
      const batchSize = 3; // Process in batches of 3
      const batchTimeout = 1000; // Or after 1 second
      let batchTimer: NodeJS.Timeout | null = null;

      const processBatch = () => {
        if (batch.length > 0) {
          handler([...batch]);
          batch.length = 0;
        }
        if (batchTimer) {
          clearTimeout(batchTimer);
          batchTimer = null;
        }
      };

      // Store batch handler
      this.eventHandlers.set(`batch:${eventKey}`, (eventData: EventBase<MessageType, HeadersType>) => {
        batch.push(eventData);

        if (batch.length >= batchSize) {
          processBatch();
        } else if (!batchTimer) {
          batchTimer = setTimeout(processBatch, batchTimeout);
        }
      });

      // If this is the first subscription, start the consumer
      if (this.subscriptions.size === 0) {
        await this.consumer.subscribe({ topic: topicName, fromBeginning: false });

        await this.consumer.run({
          eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
            try {
              const eventKey = message.key?.toString();
              if (eventKey) {
                const eventData: EventBase<MessageType, HeadersType> = JSON.parse(message.value?.toString() || '{}');
                
                // Check batch handlers
                for (const [handlerKey, handlerFn] of this.eventHandlers.entries()) {
                  if (handlerKey.startsWith('batch:')) {
                    const batchEventKey = handlerKey.substring(6); // Remove 'batch:' prefix
                    if (batchEventKey === eventKey) {
                      handlerFn(eventData, eventKey);
                    }
                  } else if (handlerKey.startsWith('pattern:')) {
                    const pattern = handlerKey.substring(8); // Remove 'pattern:' prefix
                    const regex = new RegExp(pattern.replace(/\*/g, '.*'));
                    if (regex.test(eventKey)) {
                      handlerFn(eventData, eventKey);
                    }
                  } else if (handlerKey === eventKey) {
                    // Regular event handler
                    handlerFn(eventData, eventKey);
                  }
                }
              }
            } catch (error) {
              console.error('Error processing message:', error);
            }
          }
        });
      }

      this.subscriptions.set(subscriptionId, `batch:${eventKey}`);
      return subscriptionId;
      
    } catch (error) {
      throw new Error(`Failed to subscribe to batch: ${error}`);
    }
  }

  /**
   * Converts headers object to Kafka headers format.
   * 
   * @private
   * @param headers - Headers object
   * @returns Kafka headers object
   */
  private convertHeadersToKafkaHeaders(headers: HeadersType): Record<string, string> {
    const kafkaHeaders: Record<string, string> = {};
    
    if (headers && typeof headers === 'object') {
      for (const [key, value] of Object.entries(headers)) {
        kafkaHeaders[key] = String(value);
      }
    }
    
    return kafkaHeaders;
  }

  /**
   * Generates a unique subscription ID for tracking subscriptions.
   * 
   * @private
   * @returns A unique subscription ID string
   */
  private generateSubscriptionId(): string {
    return `sub_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }
}
