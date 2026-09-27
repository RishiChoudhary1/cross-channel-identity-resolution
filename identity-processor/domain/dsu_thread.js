const { parentPort } = require('worker_threads');
const nativeEngine = require('../build/Release/identity_dsu_engine.node');

parentPort.on('message', (task) => {
    // We now receive the data AND a dedicated reply port
    const { edges, port } = task;
    
    try {
        const dsuResults = nativeEngine.mergeDisjointClusters(edges);
        port.postMessage({ success: true, data: dsuResults });
    } catch (error) {
        port.postMessage({ success: false, error: error.message });
    }
});