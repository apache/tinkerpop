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

// request represents a request to the server.
type request struct {
	requestID uuid.UUID
	op        string
	processor string
	args      map[string]interface{}
}

const sessionProcessor = "session"

const stringOp = "eval"
const stringProcessor = ""

// Bindings should be a key-object map (different from Binding class in bytecode).
func makeStringRequest(stringGremlin string, traversalSource string, sessionId string, bindings ...map[string]interface{}) (req request) {
	newProcessor := stringProcessor
	newArgs := map[string]interface{}{
		"gremlin": stringGremlin,
		"aliases": map[string]interface{}{
			"g": traversalSource,
		},
	}
	if sessionId != "" {
		newProcessor = sessionProcessor
		newArgs["session"] = sessionId
	}
	if len(bindings) > 0 {
		newArgs["bindings"] = bindings[0]
	}
	return request{
		requestID: uuid.New(),
		op:        stringOp,
		processor: newProcessor,
		args:      newArgs,
	}
}

const bytecodeOp = "bytecode"
const bytecodeProcessor = "traversal"
const authOp = "authentication"
const authProcessor = "traversal"

func makeBytecodeRequest(bytecodeGremlin *bytecode, traversalSource string, sessionId string) (req request) {
	newProcessor := bytecodeProcessor
	newArgs := map[string]interface{}{
		"gremlin": *bytecodeGremlin,
		"aliases": map[string]interface{}{
			"g": traversalSource,
		},
	}
	if sessionId != "" {
		newProcessor = sessionProcessor
		newArgs["session"] = sessionId
	}
	return request{
		requestID: uuid.New(),
		op:        bytecodeOp,
		processor: newProcessor,
		args:      newArgs,
	}
}

func makeBasicAuthRequest(auth string) (req request) {
	return request{
		requestID: uuid.New(),
		op:        authOp,
		processor: authProcessor,
		args: map[string]interface{}{
			"sasl": auth,
		},
	}
}

func makeCloseSessionRequest(sessionId string) request {
	return request{
		requestID: uuid.New(),
		op:        "close",
		processor: "session",
		args: map[string]interface{}{
			"session": sessionId,
		},
	}
}
