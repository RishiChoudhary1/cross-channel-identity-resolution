const { createClient } = require('redis');

const redisClient = createClient({
  url: 'redis://localhost:6379' 
});

redisClient.on('error', (err) => console.error('[Cache] Redis Client Error:', err));

const connectRedisCache = async () => {
  try {
    await redisClient.connect();
    console.log('[Cache] Redis Graph Cache connected successfully.');
  } catch (error) {
    console.error('[Cache] Failed to connect to Redis:', error);
    process.exit(1); 
  }
};

// TIER 1 DSU: We now store Graph Nodes instead of flat mappings.
// Each node needs a 'parent' (for path traversal) and a 'rank' (for tree balancing).
const getGraphNode = async (identifierKey) => {
  const node = await redisClient.hGetAll(identifierKey);
  
  if (Object.keys(node).length === 0) {
    return null; // Node doesn't exist yet
  }
  
  return {
    parent: node.parent,
    rank: parseInt(node.rank, 10),
    lastSeen: parseInt(node.lastSeen, 10) // Crucial for our chronological time-decay algorithm
  };
};

// Bulk updates the graph edges after the C++ worker computes the Union
const updateGraphEdges = async (updates) => {
  const multi = redisClient.multi(); // Use Redis transactions to update all edges atomically
  
  for (const { key, parent, rank, timestamp } of updates) {
    multi.hSet(key, {
      parent: parent,
      rank: rank.toString(),
      lastSeen: timestamp.toString()
    });
    // Keep nodes alive for 30 days
    multi.expire(key, 2592000); 
  }

  await multi.exec();
};


// TIER 1: Pipelined fetch to eliminate N+1 network overhead
const getGraphNodesBatch = async (identifierKeys) => {
  if (identifierKeys.length === 0) return [];
  
  const multi = redisClient.multi();
  
  // Queue up all the hGetAll commands in memory
  identifierKeys.forEach(key => multi.hGetAll(key));
  
  // Execute them in a single network round-trip
  const results = await multi.exec();
  
  return results.map((node, index) => {
    if (Object.keys(node).length === 0) return null;
    return {
      key: identifierKeys[index],
      parent: node.parent,
      rank: parseInt(node.rank, 10),
      lastSeen: parseInt(node.lastSeen, 10)
    };
  });
};

// In infrastructure/redis.adapter.js
const resolveTrueRoot = async (initialParent) => {
  let currentId = initialParent;
  
  // Safeguard loop to prevent infinite cycles in case of data corruption
  for (let depth = 0; depth < 10; depth++) {
    // Check if this cluster has been merged into a newer root
    const forwardPointer = await redisClient.get(`forward:${currentId}`);
    if (!forwardPointer) {
      return currentId; // We found the absolute root
    }
    currentId = forwardPointer;
  }
  return currentId;
};

module.exports = {
  connectRedisCache,
  getGraphNode,
  updateGraphEdges,
  getGraphNodesBatch,
  resolveTrueRoot,
};