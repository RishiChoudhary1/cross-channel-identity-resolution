const mongoose = require('mongoose');
const crypto = require('crypto');

// 1. Define the Schema for Storing Processed Events
const ProcessedEventSchema = new mongoose.Schema({
  // The master identifier this event ultimately resolved to
  globalUserId: { 
    type: String, 
    required: true, 
    index: true // Indexed because analysts will query: "Show me all events for User X"
  },
  
  originalEventId: { type: String, required: true },
  source: { type: String, required: true },
  timestamp: { type: Date, required: true },
  action: { type: String, required: true },
  
  // Storing the original identifiers and payload for explainability and auditing
  identifiers: { type: mongoose.Schema.Types.Mixed },
  payload: { type: mongoose.Schema.Types.Mixed },
  
  // Tier 1 Practice: Idempotency Key (Duplicate Detection)
  payloadHash: { 
    type: String, 
    required: true, 
    unique: true // MongoDB will enforce this at the database engine level
  }
});

// Compound index to quickly sort a user's timeline chronologically
ProcessedEventSchema.index({ globalUserId: 1, timestamp: -1 });

const ProcessedEvent = mongoose.model('ProcessedEvent', ProcessedEventSchema);

// 2. The Connection Logic
const connectMongoStore = async () => {
  try {
    // Connect to the MongoDB container we spun up in Docker
    await mongoose.connect('mongodb://localhost:27018/identity_graph_db', {
      serverSelectionTimeoutMS: 5000,
    });
    console.log('[Storage] MongoDB Event Store connected successfully.');
  } catch (error) {
    console.error('[Storage] MongoDB connection failed:', error);
    process.exit(1);
  }
};

// 3. The Insertion Logic with Duplicate Protection
const persistEvent = async (globalUserId, rawEvent) => {
  // Create a deterministic hash of the entire event to prevent duplicates
  const hashInput = rawEvent.eventId + rawEvent.timestamp;
  const eventHash = crypto.createHash('sha256').update(hashInput).digest('hex');

  try {
    await ProcessedEvent.create({
      globalUserId,
      originalEventId: rawEvent.eventId,
      source: rawEvent.source,
      timestamp: new Date(rawEvent.timestamp),
      action: rawEvent.action,
      identifiers: rawEvent.identifiers,
      payload: rawEvent.payload,
      payloadHash: eventHash,
    });
    console.log(`[Storage] Event ${rawEvent.eventId} persisted to timeline for ${globalUserId}.`);
  } catch (error) {
    // If it's a Mongo duplicate key error (code 11000), we handle it gracefully
    if (error.code === 11000) {
      console.warn(`[Storage] Duplicate event detected and dropped: ${rawEvent.eventId}`);
      return; 
    }
    throw error;
  }
};

// In infrastructure/storage.adapter.js
const fetchCustomerJourney = async (globalUserId, cursorId = null, limit = 50) => {
  const query = { globalUserId };
  
  if (cursorId) {
    // Only fetch documents older than the provided cursor
    query._id = { $lt: cursorId }; 
  }

  return await ProcessedEvent.find(query)
    .sort({ _id: -1 }) // Fetch newest events first
    .limit(limit)
    .lean()
    .exec();
};

module.exports = {
  connectMongoStore,
  persistEvent,
  fetchCustomerJourney,
};