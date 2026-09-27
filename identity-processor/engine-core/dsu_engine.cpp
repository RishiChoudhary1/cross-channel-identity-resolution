#include <napi.h>
#include <unordered_map>
#include <string>

class IdentityGraph {
private:
    std::unordered_map<std::string, std::string> parent;
    std::unordered_map<std::string, int> rank;

public:
    // Memory Optimization: Pre-allocate map buckets to prevent expensive rehashing
    void reserveMemory(size_t nodeCount) {
        parent.reserve(nodeCount);
        rank.reserve(nodeCount);
    }

    // Pass by const reference to avoid deep string copies in memory
    std::string findNode(const std::string& i) {
        if (parent.find(i) == parent.end()) {
            parent[i] = i;
            rank[i] = 0;
            return i;
        }
        if (parent[i] == i) {
            return i;
        }
        // Path compression
        return parent[i] = findNode(parent[i]); 
    }

    void uniteNodes(const std::string& i, const std::string& j) {
        std::string root_i = findNode(i);
        std::string root_j = findNode(j);

        if (root_i != root_j) {
            if (rank[root_i] < rank[root_j]) {
                parent[root_i] = root_j;
            } else if (rank[root_i] > rank[root_j]) {
                parent[root_j] = root_i;
            } else {
                parent[root_j] = root_i;
                rank[root_i]++;
            }
        }
    }

    const std::unordered_map<std::string, std::string>& getGraphState() {
        for (auto const& [node, _] : parent) {
            findNode(node);
        }
        return parent;
    }
};

Napi::Value MergeDisjointClusters(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    // 1. Type Validation
    if (info.Length() < 1 || !info[0].IsArray()) {
        Napi::TypeError::New(env, "Expected a flat array of edge pairs").ThrowAsJavaScriptException();
        return env.Null();
    }

    Napi::Array flatEdgesArray = info[0].As<Napi::Array>();
    uint32_t length = flatEdgesArray.Length();
    
    // 2. TIER 1: Strict Memory Bounds Validation
    // Prevents V8 out-of-bounds access on i + 1
    if (length % 2 != 0) {
        Napi::TypeError::New(env, "Edge array length must be even (pairs of nodes)").ThrowAsJavaScriptException();
        return env.Null();
    }
    
    IdentityGraph graph;
    graph.reserveMemory(length);

    // 3. Safe Traversal
    for (uint32_t i = 0; i < length; i += 2) {
        std::string nodeA = flatEdgesArray.Get(i).As<Napi::String>().Utf8Value();
        std::string nodeB = flatEdgesArray.Get(i + 1).As<Napi::String>().Utf8Value();
        
        graph.uniteNodes(nodeA, nodeB);
    }

    Napi::Object result = Napi::Object::New(env);
    const auto& finalState = graph.getGraphState();
    
    for (auto const& [node, root] : finalState) {
        result.Set(node, root);
    }

    return result;
}

Napi::Object Init(Napi::Env env, Napi::Object exports) {
    exports.Set(Napi::String::New(env, "mergeDisjointClusters"),
                Napi::Function::New(env, MergeDisjointClusters));
    return exports;
}

NODE_API_MODULE(identity_dsu_engine, Init)