package gremlingo

const (
	BaseNamespace         = "org.apache.tinkerpop.gremlin.process.traversal.strategy."
	DecorationNamespace   = BaseNamespace + "decoration."
	FinalizationNamespace = BaseNamespace + "finalization."
	OptimizationNamespace = BaseNamespace + "optimization."
	VerificationNamespace = BaseNamespace + "verification."
)

func PartitionStrategy(partitionKey, writePartition, readPartitions, includeMetaProperties string) *TraversalStrategy {
	config := make(map[string]string)
	if partitionKey != "" {
		config["partitionKey"] = partitionKey
	}
	if writePartition != "" {
		config["writePartition"] = writePartition
	}
	if readPartitions != "" {
		config["readPartitions"] = readPartitions
	}
	if includeMetaProperties != "" {
		config["includeMetaProperties"] = includeMetaProperties
	}
	return &TraversalStrategy{name: DecorationNamespace + "PartitionStrategy", configuration: config}
}
