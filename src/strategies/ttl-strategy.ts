import { HandlerExecutionError } from "@soapjs/soap";
import { EventBase } from "@soapjs/soap";
import { KafkaProcessingStrategy } from "./base-strategy";

/**
 * TTL (Time-To-Live) Strategy for Kafka.
 * 
 * This strategy implements message expiration based on TTL.
 * Note: Actual TTL handling is done by Kafka message retention.
 */
export class TTLStrategy<
  MessageType,
  HeadersType = Record<string, unknown>
> extends KafkaProcessingStrategy<MessageType, HeadersType> {

  private defaultTTL: number;

  constructor(
    options: {
      defaultTTL?: number;
      deadLetterTopic?: string;
    } = {}
  ) {
    super({ deadLetterTopic: options.deadLetterTopic });
    this.defaultTTL = options.defaultTTL || 300000; // 5 minutes default
  }

  async process(
    message: EventBase<MessageType, HeadersType>,
    handler: (event: EventBase<MessageType, HeadersType>) => Promise<void>
  ): Promise<void> {
    this.validateMessage(message);
    
    const ttl = this.getTTL(message);
    const messageAge = this.getMessageAge(message);
    
    if (messageAge > ttl) {
      console.warn(`Message expired (TTL: ${ttl}ms, Age: ${messageAge}ms)`);
      throw new HandlerExecutionError(
        `Message expired (TTL: ${ttl}ms, Age: ${messageAge}ms)`
      );
    }
    
    try {
      await handler(message);
      console.log(`Message processed within TTL (${ttl}ms remaining)`);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(`TTL processing failed: ${errorMessage}`);
      
      throw new HandlerExecutionError(
        `TTL processing failed: ${errorMessage}`
      );
    }
  }

  private getTTL(message: EventBase<MessageType, HeadersType>): number {
    return (message.headers as any)?.['x-ttl'] || this.defaultTTL;
  }

  private getMessageAge(message: EventBase<MessageType, HeadersType>): number {
    const timestamp = (message.headers as any)?.['x-timestamp'];
    if (!timestamp) return 0;
    
    return Date.now() - new Date(timestamp).getTime();
  }
}
