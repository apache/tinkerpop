/*
Licensed to the Apache Software Foundation (ASF) under one
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

type Traversal struct {
	graph               *Graph
	traversalStrategies *TraversalStrategies
	bytecode            *bytecode
}

const host string = "localhost"
const port int = 8182

// TODO use TraversalStrategies instead of direct remote after they are implemented
var remote = DriverRemoteConnection{
	client: NewClient(host, port),
}

// ToList returns the result in a list
// TODO commenting out as remote.Submit doesn't support bytecode yet
//func (t *Traversal) ToList() []*Result {
//	results, _ := remote.Submit(t.bytecode)
//	return results.All()
//}

// ToSet returns the results in a set.
// TODO Go doesn't have sets, determine the best structure for this
//func (t *Traversal) ToSet() map[*Result]bool {
//	set := make(map[*Result]bool)
//	results, _ := remote.Submit(t.bytecode)
//	for _, r := range results.All() {
//		set[r] = true
//	}
//	return set
//}

// Iterate iterate all the Traverser instances in the traversal and returns the empty traversal
// TODO complete this when Traverser is implemented
func (t *Traversal) Iterate() *Traversal {
	t.bytecode.addStep("none")
	return t
}
