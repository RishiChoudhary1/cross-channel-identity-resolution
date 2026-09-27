const { Kafka, Partitioners } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'edge-gateway-producer',
  brokers: ['localhost:9092'], 
  retry: {
    initialRetryTime: 100,
    retries: 8 
  }
});

// Create the Admin client to manage infrastructure
const admin = kafka.admin();
const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });

const connectKafkaProducer = async () => {
  try {
    // 1. Connect Admin and ensure topic exists BEFORE producer connects
    await admin.connect();
    await admin.createTopics({
      topics: [{
        topic: 'raw-customer-events',
        numPartitions: 3, // Prepares us for Tier 3: Parallel processing across multiple workers
        replicationFactor: 1
      }],
      waitForLeaders: true,
    });
    console.log('[Kafka] Topic ensured: raw-customer-events with 3 partitions.');
    await admin.disconnect();

    // 2. Connect Producer
    await producer.connect();
    console.log('[Kafka] Producer connected successfully.');
  } catch (error) {
    console.error('[Kafka] Failed to initialize Kafka:', error);
    process.exit(1); 
  }
};

const publishEvent = async (topic, eventPayload) => {
  try {
    const idents = eventPayload.identifiers || {};
    const routingKey = idents.registeredUserId || 
                       idents.email || 
                       idents.phone || 
                       idents.cookieId || 
                       idents.deviceId || 
                       eventPayload.eventId;

    await producer.send({
      topic,
      messages: [
        { 
          key: routingKey, 
          value: JSON.stringify(eventPayload) 
        },
      ],
    });
  } catch (error) {
    console.error(`[Kafka] Failed to publish event to topic ${topic}:`, error);
    throw error; 
  }
};

module.exports = {
  connectKafkaProducer,
  publishEvent,
};