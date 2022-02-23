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

import "github.com/google/uuid"

// request represents a request to the server
type request struct {
	requestID uuid.UUID
	op        string
	processor string
	args      map[string]interface{}
}

const stringOp = "eval"
const stringProcessor = ""

func makeStringRequest(stringGremlin string) (req request) {
	return request{
		requestID: uuid.New(),
		op:        stringOp,
		processor: stringProcessor,
		args: map[string]interface{}{
			"gremlin": stringGremlin,
			"aliases": map[string]interface{}{
				"g": "g",
			},
		},
	}
}

const bytecodeOp = "bytecode"
const bytecodeProcessor = "traversal"

func makeBytecodeRequest(bytecodeGremlin *bytecode) (req request) {
	return request{
		requestID: uuid.New(),
		op:        bytecodeOp,
		processor: bytecodeProcessor,
		args: map[string]interface{}{
			"gremlin": *bytecodeGremlin,
			"aliases": map[string]interface{}{
				"g": "g",
			},
		},
	}
}

// TODO: AN-1029 - Enable configurable request aliases
