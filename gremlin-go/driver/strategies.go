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
	baseNamespace         = "org.apache.tinkerpop.gremlin.process.traversal.strategy."
	decorationNamespace   = baseNamespace + "decoration."
	finalizationNamespace = baseNamespace + "finalization."
	optimizationNamespace = baseNamespace + "optimization."
	verificationNamespace = baseNamespace + "verification."
)

func PartitionStrategy(partitionKey, writePartition, readPartitions, includeMetaProperties string) *traversalStrategy {
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
	return &traversalStrategy{name: decorationNamespace + "PartitionStrategy", configuration: config}
}

func ReadOnlyStrategy() *traversalStrategy {
	return &traversalStrategy{name: verificationNamespace + "ReadOnlyStrategy"}
}

func RemoteStrategy(connection DriverRemoteConnection) *traversalStrategy {
	a := func(g GraphTraversal) {
		result, err := g.getResults()
		if err != nil || result != nil {
			return
		}

		rs, err := connection.submitBytecode(g.bytecode)
		if err != nil {
			return
		}
		g.results = rs
	}

	return &traversalStrategy{name: "RemoteStrategy", apply: a}
}
