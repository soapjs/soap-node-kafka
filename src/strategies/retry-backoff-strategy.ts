import { HandlerExecutionError } from "@soapjs/soap";
import { EventBase } from "@soapjs/soap";
import { KafkaProcessingStrategy } from "./base-strategy";

/**
 * Retry with Exponential Backoff Strategy for Kafka.
 * 
 * This strategy implements retry logic with exponential backoff.
 * The actual retry mechanism is handled by EventProcessor.
 */
export class RetryWithBackoffStrategy<
  MessageType,
  HeadersType = Record<string, unknown>
> extends KafkaProcessingStrategy<MessageType, HeadersType> {

  private maxRetries: number;
  private baseDelay: number;

  constructor(
    options: {
      maxRetries?: number;
      baseDelay?: number;
      retryTopic?: string;
      deadLetterTopic?: string;
    } = {}
  ) {
    super(options);
    this.maxRetries = options.maxRetries || 3;
    this.baseDelay = options.baseDelay || 1000;
  }

  async process(
    message: EventBase<MessageType, HeadersType>,
    handler: (event: EventBase<MessageType, HeadersType>) => Promise<void>
  ): Promise<void> {
    try {
      this.validateMessage(message);
      await handler(message);
    } catch (error) {
      const retryCount = this.getRetryCount(message);
      
      if (retryCount < this.maxRetries) {
        const delay = this.calculateBackoffDelay(retryCount);
        console.log(`Message will be retried ${retryCount + 1}/${this.maxRetries} in ${delay}ms`);
        
        throw new HandlerExecutionError(
          `Message processing failed, will be retried: ${error}`
        );
      } else {
        console.error(`Max retries (${this.maxRetries}) exceeded, message will be routed to DLQ`);
        
        throw new HandlerExecutionError(
          `Max retries exceeded: ${error}`
        );
      }
    }
  }

  private getRetryCount(message: EventBase<MessageType, HeadersType>): number {
    return (message.headers as any)?.['x-retry-count'] || 0;
  }

  private calculateBackoffDelay(retryCount: number): number {
    return this.baseDelay * Math.pow(2, retryCount);
  }
}
