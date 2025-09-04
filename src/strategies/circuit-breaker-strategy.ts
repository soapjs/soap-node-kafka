import { HandlerExecutionError } from "@soapjs/soap";
import { EventBase } from "@soapjs/soap";
import { KafkaProcessingStrategy } from "./base-strategy";

/**
 * Circuit Breaker Strategy for Kafka.
 * 
 * This strategy implements the circuit breaker pattern to prevent
 * cascading failures and provide graceful degradation.
 */
export class CircuitBreakerStrategy<
  MessageType,
  HeadersType = Record<string, unknown>
> extends KafkaProcessingStrategy<MessageType, HeadersType> {

  private failureCount: number = 0;
  private lastFailureTime: number = 0;
  private state: 'CLOSED' | 'OPEN' | 'HALF_OPEN' = 'CLOSED';
  private failureThreshold: number;
  private recoveryTimeout: number;

  constructor(
    options: {
      failureThreshold?: number;
      recoveryTimeout?: number;
      deadLetterTopic?: string;
    } = {}
  ) {
    super({ deadLetterTopic: options.deadLetterTopic });
    this.failureThreshold = options.failureThreshold || 5;
    this.recoveryTimeout = options.recoveryTimeout || 60000; // 1 minute
  }

  async process(
    message: EventBase<MessageType, HeadersType>,
    handler: (event: EventBase<MessageType, HeadersType>) => Promise<void>
  ): Promise<void> {
    this.validateMessage(message);
    
    if (this.state === 'OPEN') {
      if (Date.now() - this.lastFailureTime > this.recoveryTimeout) {
        this.state = 'HALF_OPEN';
        console.log('Circuit breaker transitioning to HALF_OPEN state');
      } else {
        console.warn('Circuit breaker is OPEN, message will be routed to DLQ');
        throw new HandlerExecutionError(
          'Circuit breaker is OPEN, message will be routed to dead letter queue'
        );
      }
    }
    
    try {
      await handler(message);
      
      // Success - reset circuit breaker if it was half-open
      if (this.state === 'HALF_OPEN') {
        this.state = 'CLOSED';
        this.failureCount = 0;
        console.log('Circuit breaker reset to CLOSED state');
      }
    } catch (error) {
      this.failureCount++;
      this.lastFailureTime = Date.now();
      
      if (this.failureCount >= this.failureThreshold) {
        this.state = 'OPEN';
        console.error(`Circuit breaker opened after ${this.failureCount} failures`);
      }
      
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(`Circuit breaker failure (${this.failureCount}/${this.failureThreshold}): ${errorMessage}`);
      
      throw new HandlerExecutionError(
        `Circuit breaker failure: ${errorMessage}`
      );
    }
  }
}
