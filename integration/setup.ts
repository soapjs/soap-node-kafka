// Polyfill for crypto in Node.js environment
import { webcrypto } from 'crypto';
if (!globalThis.crypto) {
  globalThis.crypto = webcrypto as any;
}

// Integration test setup - uses shared Kafka container
const setupIntegrationTests = async (): Promise<string[]> => {
  // Get broker URLs from environment variable (set by the test runner script)
  const brokerUrls = process.env.KAFKA_BROKER_URLS;
  
  if (!brokerUrls) {
    throw new Error('KAFKA_BROKER_URLS environment variable is not set. Please run tests using the integration test script.');
  }
  
  const brokers = brokerUrls.split(',');
  console.log(`Using Kafka brokers: ${brokers.join(', ')}`);
  return brokers;
};

const teardownIntegrationTests = async (): Promise<void> => {
  // No teardown needed - container is managed by the test runner script
  console.log('Test completed, container will be managed by test runner');
};

// Export setup and teardown functions for use in individual tests
export { setupIntegrationTests, teardownIntegrationTests };
