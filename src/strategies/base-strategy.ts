/* eslint-disable @typescript-eslint/no-unused-vars */
import {
  EventValidationError,
  EventParsingError,
  HandlerExecutionError,
} from "@soapjs/soap";
import { EventBase } from "@soapjs/soap";
import { EventProcessingStrategy } from "@soapjs/soap";

/**
 * Base class for Kafka-specific processing strategies.
 * Provides common functionality for Kafka message handling.
 */
export abstract class KafkaProcessingStrategy<
  MessageType,
  HeadersType = Record<string, unknown>
> implements EventProcessingStrategy<MessageType, HeadersType> {
  
  protected deadLetterTopic?: string;
  protected retryTopic?: string;

  constructor(
    protected options: {
      deadLetterTopic?: string;
      retryTopic?: string;
      maxRetries?: number;
      retryDelay?: number;
    } = {}
  ) {
    this.deadLetterTopic = options.deadLetterTopic;
    this.retryTopic = options.retryTopic;
  }

  /**
   * Abstract method to be implemented by concrete strategies.
   */
  abstract process(
    message: EventBase<MessageType, HeadersType>,
    handler: (event: EventBase<MessageType, HeadersType>) => Promise<void>
  ): Promise<void>;

  /**
   * Validates the event message to ensure it meets required criteria.
   */
  protected validateMessage(message: EventBase<MessageType, HeadersType>): void {
    if (!message) {
      throw new EventValidationError("Message validation failed");
    }
    if (!message.message) {
      throw new EventValidationError("Message payload is required");
    }
  }
}
