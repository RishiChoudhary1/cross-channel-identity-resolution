//this is the primary server where i reciever diff type of events

const express = require('express');
const crypto = require('crypto'); // Built-in Node.js module (Modern practice: replaces 'uuid' npm package)
const { NormalizedEventSchema } = require('../shared-contracts/event.schema');
const { connectKafkaProducer, publishEvent } = require('./kafka.client');
const { connectMongoStore, fetchCustomerJourney } = require('../identity-processor/infrastructure/storage.adapter');
const { connectRedisCache, getGraphNode, resolveTrueRoot } = require('../identity-processor/infrastructure/redis.adapter');



const app = express();
app.use(express.json()); // Built-in middleware to parse JSON request bodies

// 1. Health Check Endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'healthy', timestamp: new Date().toISOString() });
});

// 2. The Main Ingestion Endpoint
app.post('/api/v1/events/ingest', async (req, res) => {
  try {
    // A. Augment payload with server-side trusted data
    // Never trust the client entirely. If they don't send an ID or timestamp, we generate one.
    const incomingData = {
      ...req.body,
      eventId: req.body.eventId || crypto.randomUUID(), 
      timestamp: req.body.timestamp || new Date().toISOString(),
    };

    // B. Validate using our Shared Contract (Zod)
    // We use .safeParse() instead of .parse() so it doesn't throw a crash-inducing error
    const validationResult = NormalizedEventSchema.safeParse(incomingData);

    if (!validationResult.success) {
       // 400 Bad Request: The client sent malformed data
       return res.status(400).json({
         error: 'Invalid event payload',
         details: validationResult.error.issues,
       });
    }

    const validEvent = validationResult.data;

    // C. Publish to Kafka
    // We wait for Kafka to acknowledge receipt before responding to the user
    await publishEvent('raw-customer-events', validEvent);

    // D. Return Success
    // 202 Accepted is the correct REST standard here. It means "We received it and will process it later."
    return res.status(202).json({
      message: 'Event ingested and buffered successfully',
      eventId: validEvent.eventId,
    });

  } catch (error) {
    console.error('[Gateway] Ingestion Error:', error);
    // 500 Internal Server Error: Something broke on our end (e.g., Kafka is down)
    return res.status(500).json({ error: 'Internal Server Error' });
  }
});


// to get the user journey route
app.get('/api/v1/identity/:type/:value', async (req, res) => {
  try {
    const { type, value } = req.params;
    // Accept an optional cursor query parameter for pagination
    const cursor = req.query.cursor || null; 
    const limit = parseInt(req.query.limit) || 50;
    
    const cacheKey = `ident:${type}:${value}`;

    // 1. Resolve Root Global User ID
    let resolvedGlobalId;
    if (type === 'globalUserId') {
      resolvedGlobalId = await resolveTrueRoot(value);
    } else {
      const node = await getGraphNode(cacheKey);
      if (!node || !node.parent) {
        return res.status(404).json({ error: 'Identity Not Found' });
      }
      // DSU Trap Fix: Traverse to the absolute root
      resolvedGlobalId = await resolveTrueRoot(node.parent);
    }

    // 2. Fetch Paginated Journey (OOM Fix)
    const events = await fetchCustomerJourney(resolvedGlobalId, cursor, limit);

    // 3. Extract the next cursor for the client
    const nextCursor = events.length === limit ? events[events.length - 1]._id : null;

    // 4. Return Safe Payload
    return res.status(200).json({
      globalUserId: resolvedGlobalId,
      eventsReturned: events.length,
      nextCursor: nextCursor,
      timeline: events.map(evt => ({
        id: evt._id, // Required for the client to pass back as the cursor
        source: evt.source,
        action: evt.action,
        timestamp: evt.timestamp,
        payload: evt.payload,
      })),
    });
  } catch (error) {
    console.error('[API] Error fetching identity journey:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

const PORT = process.env.PORT || 3000;

// 3. Graceful Startup Sequence
const startServer = async () => {
  // CRITICAL: Connect to all infrastructure before opening the port
  await connectKafkaProducer();
  await connectMongoStore();
  await connectRedisCache();
  
  app.listen(PORT, () => {
    console.log(`[Gateway] Ingress service listening on port ${PORT}`);
    console.log(`[Gateway] Customer 360 Read API is active`);
  });
};

startServer();