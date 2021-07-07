namespace java org.apache.tinkerpop.gremlin.language.examples.thrift

include "../../../src/gen/thrift/org/apache/tinkerpop/gremlin/language/property_graphs.thrift"

exception AddGraphException {
    1: string message;
}

struct AddGraphResponse {
    1: i64 totalVerticesAfterOperation;
    2: i64 totalEdgesAfterOperation;
    3: optional string message;
}

service ExampleService {
    AddGraphResponse addGraph(1: property_graphs.Graph graph) throws (1: AddGraphException error)
}
