const { Kafka } = require('kafkajs');
const { Worker, MessageChannel } = require('worker_threads');
const path = require('path');
const { connectMongoStore, persistEvent } = require('./infrastructure/storage.adapter');
const { connectRedisCache, getGraphNodesBatch, updateGraphEdges } = require('./infrastructure/redis.adapter');

// 1. Spin up the background thread for the C++ engine
const dsuWorker = new Worker(path.join(__dirname, 'domain/dsu_thread.js'));

// Promisify the worker message passing to keep our async flow clean
const runNativeDSUAsync = (flatEdges) => {
  return new Promise((resolve, reject) => {
    // Create a unique, isolated communication tube for this specific event
    const { port1, port2 } = new MessageChannel();

    port1.once('message', (response) => {
      if (response.success) resolve(response.data);
      else reject(new Error(response.error));
      
      // Close the port to prevent memory leaks in the V8 engine
      port1.close(); 
    });

    // Send the data AND hand over ownership of port2 to the worker
    dsuWorker.postMessage({ edges: flatEdges, port: port2 }, [port2]);
  });
};

// 2. Kafka Consumer Configuration
const kafka = new Kafka({
  clientId: 'identity-processor-worker',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'identity-graph-group' });

// 3. Independent Probability Math Configuration
const WEIGHTS = {
  registeredUserId: 1.0,
  email: 1.0,
  phone: 1.0,
  accountId: 1.0,
  deviceId: 0.7,
  cookieId: 0.6,
  ipAddress: 0.2,
  userAgent: 0.1,
  location: 0.1,
};

const MERGE_THRESHOLD = 0.85;

const calculateProbability = (activeWeights) => {
  if (activeWeights.length === 0) return 0;
  let inverseProduct = 1;
  for (const w of activeWeights) {
    inverseProduct *= (1 - w);
  }
  return 1 - inverseProduct;
};

// 4. Main Processor Loop
const runIdentityProcessor = async () => {
  await connectMongoStore();
  await connectRedisCache();
  
  await consumer.connect();
  await consumer.subscribe({ topic: 'raw-customer-events', fromBeginning: true });

  console.log('[Worker] Identity Processor running. C++ Thread isolated.');

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const rawEvent = JSON.parse(message.value.toString());
        const identifiers = rawEvent.identifiers || {};
        const now = Date.now();

        // Prepare keys for batch fetching
        const keysToFetch = Object.entries(identifiers)
          .filter(([_, value]) => value)
          .map(([type, value]) => `ident:${type}:${value}`);

        // Execute the single network round-trip (O(1) N+1 fix)
        const cachedNodes = await getGraphNodesBatch(keysToFetch);

        let activeWeights = [];
        let candidateClusters = new Set();

        // Process the batched results
        cachedNodes.forEach(node => {
          if (node) {
            // Extract the original type from the cache key (e.g., 'ident:email:xyz')
            const type = node.key.split(':')[1];
            const baseWeight = WEIGHTS[type] || 0.1;
            
            // Apply time-decay math
            let effectiveWeight = baseWeight;
            if (['cookieId', 'ipAddress', 'deviceId', 'userAgent', 'location'].includes(type)) {
              const elapsedDays = (now - node.lastSeen) / (1000 * 60 * 60 * 24);
              effectiveWeight = baseWeight * Math.exp(-0.05 * elapsedDays);
            }
            
            activeWeights.push(effectiveWeight);
            candidateClusters.add(node.parent);
          }
        });

        const totalProbability = calculateProbability(activeWeights);
        let resolvedGlobalId;

        // Graph Resolution
        if (totalProbability >= MERGE_THRESHOLD && candidateClusters.size > 0) {
          const clustersArray = Array.from(candidateClusters);
          if (clustersArray.length === 1) {
            resolvedGlobalId = clustersArray[0];
          } else {
            console.warn(`[Worker] Conflict detected across clusters: ${clustersArray.join(', ')}. Invoking C++ Thread.`);
            let flatEdgesForCpp = [];
            for (let i = 0; i < clustersArray.length - 1; i++) {
              flatEdgesForCpp.push(clustersArray[i], clustersArray[i + 1]);
            }
            
            // Offload to the non-blocking Node.js Worker Thread
            const dsuResults = await runNativeDSUAsync(flatEdgesForCpp);
            resolvedGlobalId = dsuResults[clustersArray[0]] || clustersArray[0];
          }
        } else {
          resolvedGlobalId = `USR-${rawEvent.eventId.substring(0, 8)}`;
          console.log(`[Engine] Created new identity cluster: ${resolvedGlobalId} (Probability: ${totalProbability.toFixed(2)})`);
        }

        // Update Redis Graph Edges Atomically
        const redisUpdates = [];
        for (const [type, value] of Object.entries(identifiers)) {
          if (value) {
            redisUpdates.push({
              key: `ident:${type}:${value}`,
              parent: resolvedGlobalId,
              rank: 0,
              timestamp: now
            });
          }
        }
        await updateGraphEdges(redisUpdates);

        // Persist to MongoDB Event Store with Idempotency Protection
        await persistEvent(resolvedGlobalId, rawEvent);

      } catch (err) {
        console.error('[Worker] Error processing message from Kafka:', err);
      }
    },
  });
};

runIdentityProcessor();