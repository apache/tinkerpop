/*
Licensed to the Apache Software Foundation (ASF) Under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

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

// todo: clarify. Code looks wrong
func RemoteStrategy(g GraphTraversal) *TraversalStrategy {
	a := func(connection DriverRemoteConnection) {
		if true {
			_, err := connection.submitBytecode(g.bytecode)
			if err != nil {
				return
			}
			// connection.Traversers = g.Traversers
		}
	}

	return &TraversalStrategy{name: "RemoteStrategy", apply: a}
}
