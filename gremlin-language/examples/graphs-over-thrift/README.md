This directory contains a complete example of a small graph being constructed by a client
and sent to a server using Thrift over TChannel.

## Running the demo

Start the server:

```bash
gradle runServer
```

In a separate terminal, run the client.
Remove the previously generated GraphSON file, first,
if this is not the first time you are running the demo:

```bash
rm /tmp/graphs-over-thrift.json

gradle runClient
```

You should now see a new GraphSON file, `/tmp/graphs-over-thrift.json`.
This is a reconstituted version of the graph which is constructed by the client (see `ExampleGraphClient.java`).

## Generating sources

Some of the sources in this directory are hand-written, while others are generated.
Follow the steps below in order to re-create all of the generated sources.
Note: the first step requires Dragon, which is a proprietary tool.

### Generate property_graphs.thrift:

```bash
dragon transform -o Thrift -i YAML ../../src/main/yaml -d org/apache/tinkerpop/gremlin/language/property_graphs.yaml src/gen/thrift
```

### Generate client/server Java:

```bash
thrift --gen java -out src/gen/java src/gen/thrift/org/apache/tinkerpop/gremlin/language/property_graphs.thrift
thrift --gen java -out src/gen/java src/main/thrift/graphs_over_thrift.thrift
```
