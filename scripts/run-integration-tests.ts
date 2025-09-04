import { spawn } from 'child_process';
import { GenericContainer, StartedTestContainer, Wait, Network, StartedNetwork } from 'testcontainers';
import * as path from 'path';
import { readdirSync } from 'fs';

// Handle process termination gracefully
let isShuttingDown = false;

process.on('SIGINT', async () => {
  if (isShuttingDown) {
    console.log('🛑 Force exiting...');
    process.exit(1);
  }
  
  isShuttingDown = true;
  console.log('🛑 Received SIGINT, cleaning up...');
  await teardownKafka();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  if (isShuttingDown) {
    console.log('🛑 Force exiting...');
    process.exit(1);
  }
  
  isShuttingDown = true;
  console.log('🛑 Received SIGTERM, cleaning up...');
  await teardownKafka();
  process.exit(0);
});

let kafkaContainer: StartedTestContainer;
let zookeeperContainer: StartedTestContainer;
let network: StartedNetwork;
let brokerUrls: string;

async function setupKafka(): Promise<string> {
  console.log('🚀 Starting Kafka container for integration tests...');
  
  // Create a network for containers to communicate
  network = await new Network().start();
  
  // Start Zookeeper first
  zookeeperContainer = await new GenericContainer('confluentinc/cp-zookeeper:7.4.0')
    .withExposedPorts(2181)
    .withEnvironment({
      ZOOKEEPER_CLIENT_PORT: '2181',
      ZOOKEEPER_TICK_TIME: '2000'
    })
    .withNetwork(network)
    .withNetworkAliases('zookeeper')
    .withWaitStrategy(Wait.forListeningPorts())
    .start();

  // Start Kafka with simplified configuration
  kafkaContainer = await new GenericContainer('confluentinc/cp-kafka:7.4.0')
    .withExposedPorts(9092)
    .withEnvironment({
      KAFKA_BROKER_ID: '1',
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181',
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:9092',
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://127.0.0.1:9092',
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true',
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
      KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS: '300000',
      KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS: '6000',
      KAFKA_SESSION_TIMEOUT_MS: '30000',
      KAFKA_HEARTBEAT_INTERVAL_MS: '3000',
      KAFKA_NUM_PARTITIONS: '3',
      KAFKA_DEFAULT_REPLICATION_FACTOR: '1'
    })
    .withNetwork(network)
    .withNetworkAliases('kafka')
    .withWaitStrategy(Wait.forLogMessage('started (kafka.server.KafkaServer)'))
    .start();

  // Get the actual mapped port and update advertised listeners
  const mappedPort = kafkaContainer.getMappedPort(9092);
  
  // Wait a bit for Kafka to be fully ready
  await new Promise(resolve => setTimeout(resolve, 5000));
  
  // Update the advertised listeners with the actual mapped port
  try {
    await kafkaContainer.exec([
      'kafka-configs',
      '--bootstrap-server', 'kafka:9092',
      '--entity-type', 'brokers',
      '--entity-name', '1',
      '--alter',
      '--add-config', `advertised.listeners=PLAINTEXT://127.0.0.1:${mappedPort}`
    ]);
  } catch (error) {
    console.warn('⚠️ Failed to update advertised listeners, continuing anyway:', error);
  }

  brokerUrls = `127.0.0.1:${mappedPort}`;
  
  console.log(`✅ Zookeeper container started on port ${zookeeperContainer.getMappedPort(2181)}`);
  console.log(`✅ Kafka container started on port ${mappedPort}`);
  console.log(`🔗 Broker URLs: ${brokerUrls}`);
  
  return brokerUrls;
}

async function teardownKafka(): Promise<void> {
  if (kafkaContainer) {
    console.log('🛑 Stopping Kafka container...');
    await kafkaContainer.stop();
    console.log('✅ Kafka container stopped');
  }
  if (zookeeperContainer) {
    console.log('🛑 Stopping Zookeeper container...');
    await zookeeperContainer.stop();
    console.log('✅ Zookeeper container stopped');
  }
  if (network) {
    console.log('🛑 Stopping network...');
    await network.stop();
    console.log('✅ Network stopped');
  }
}

async function runTestFile(testFile: string): Promise<boolean> {
  return new Promise((resolve) => {
    console.log(`\n🧪 Running test: ${testFile}`);
    
    const testProcess = spawn('npx', [
      'jest',
      '--config=jest.config.integration.json',
      '--testPathPattern=' + testFile,
      '--verbose',
      '--forceExit'
    ], {
      stdio: 'inherit',
      env: {
        ...process.env,
        KAFKA_BROKER_URLS: brokerUrls
      }
    });

    testProcess.on('close', (code) => {
      if (code === 0) {
        console.log(`✅ Test passed: ${testFile}`);
        resolve(true);
      } else {
        console.log(`❌ Test failed: ${testFile} (exit code: ${code})`);
        resolve(false);
      }
    });

    testProcess.on('error', (error) => {
      console.error(`❌ Error running test ${testFile}:`, error);
      resolve(false);
    });
  });
}

async function runAllIntegrationTests(): Promise<void> {
  const testFiles = readdirSync(path.join(__dirname, '..', 'integration')).filter(file => file.endsWith('.test.ts'));

  try {
    // Setup Kafka container
    await setupKafka();
    
    // Wait a bit for container to be fully ready
    await new Promise(resolve => setTimeout(resolve, 15000));
    
    console.log('\n📋 Running integration tests...');
    
    const results: { file: string; passed: boolean }[] = [];
    
    for (const testFile of testFiles) {
      const passed = await runTestFile(testFile);
      results.push({ file: testFile, passed });
    }
    
    // Summary
    console.log('\n📊 Test Results Summary:');
    console.log('========================');
    
    const passed = results.filter(r => r.passed).length;
    const failed = results.filter(r => !r.passed).length;
    
    results.forEach(result => {
      const status = result.passed ? '✅ PASS' : '❌ FAIL';
      console.log(`${status} ${result.file}`);
    });
    
    console.log(`\n📈 Total: ${results.length} tests`);
    console.log(`✅ Passed: ${passed}`);
    console.log(`❌ Failed: ${failed}`);
    
    if (failed > 0) {
      console.log('\n❌ Some tests failed!');
      process.exit(1);
    } else {
      console.log('\n🎉 All tests passed!');
      process.exit(0);
    }
    
  } catch (error) {
    console.error('❌ Error during test execution:', error);
    process.exit(1);
  } finally {
    await teardownKafka();
  }
}

// Handle process termination
process.on('SIGINT', async () => {
  console.log('\n🛑 Received SIGINT, cleaning up...');
  await teardownKafka();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n🛑 Received SIGTERM, cleaning up...');
  await teardownKafka();
  process.exit(0);
});

// Run the tests
runAllIntegrationTests();
