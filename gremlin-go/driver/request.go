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

// request represents a request to the server.
type request struct {
	gremlin string
	fields  map[string]interface{}
}

// MakeStringRequest creates a request from a Gremlin string query for submission to a Gremlin server.
//
// This function is exposed publicly to enable alternative transport protocols (gRPC, HTTP/2, etc.)
// to construct properly formatted requests outside the standard WebSocket client. The returned
// request can then be serialized using SerializeMessage().
//
// Parameters:
//   - stringGremlin: The Gremlin query string to execute
//   - traversalSource: The name of the traversal source (typically "g")
//   - requestOptions: Options such as bindings, timeout, batch size, etc.
//
// Returns:
//   - request: A request structure ready for serialization
//
// Example for alternative transports:
//
//	req := MakeStringRequest("g.V().count()", "g", RequestOptions{})
//	serializer := newGraphBinarySerializer(nil)
//	bytes, _ := serializer.(graphBinarySerializer).SerializeMessage(&req)
//	// Send bytes over gRPC, HTTP/2, etc.
func MakeStringRequest(stringGremlin string, traversalSource string, requestOptions RequestOptions) (req request) {
	newFields := map[string]interface{}{
		"language": "gremlin-lang",
		"g":        traversalSource,
	}

	if requestOptions.bindings != nil {
		newFields["bindings"] = requestOptions.bindings
	}

	if requestOptions.evaluationTimeout != 0 {
		newFields["evaluationTimeout"] = requestOptions.evaluationTimeout
	}

	if requestOptions.batchSize != 0 {
		newFields["batchSize"] = requestOptions.batchSize
	}

	if requestOptions.userAgent != "" {
		newFields["userAgent"] = requestOptions.userAgent
	}

	if requestOptions.materializeProperties != "" {
		newFields["materializeProperties"] = requestOptions.materializeProperties
	}

	return request{
		gremlin: stringGremlin,
		fields:  newFields,
	}
}

// MakeBytecodeRequest creates a request from Gremlin bytecode for submission to a Gremlin server.
//
// This function is exposed publicly to enable alternative transport protocols (gRPC, HTTP/2, etc.)
// to construct properly formatted requests outside the standard WebSocket client. The returned
// request can then be serialized using SerializeMessage().
//
// Parameters:
//   - bytecodeGremlin: The Gremlin bytecode to execute
//   - traversalSource: The name of the traversal source (typically "g")
//
// Returns:
//   - request: A request structure ready for serialization
//
// Example for alternative transports:
//
//	bytecode := g.V().HasLabel("person").Bytecode
//	req := MakeBytecodeRequest(bytecode, "g")
//	serializer := newGraphBinarySerializer(nil)
//	bytes, _ := serializer.(graphBinarySerializer).SerializeMessage(&req)
//	// Send bytes over gRPC, HTTP/2, etc.
func MakeBytecodeRequest(bytecodeGremlin *Bytecode, traversalSource string) (req request) {
	newFields := map[string]interface{}{
		"gremlin": *bytecodeGremlin,
		"aliases": map[string]interface{}{
			"g": traversalSource,
		},
	}

	for k, v := range extractReqArgs(bytecodeGremlin) {
		newFields[k] = v
	}

	return request{
		fields: newFields,
	}
}

// allowedReqArgs contains the arguments that will be extracted from the
// bytecode and sent with the request.
var allowedReqArgs = map[string]bool{
	"evaluationTimeout":     true,
	"batchSize":             true,
	"requestId":             true,
	"userAgent":             true,
	"materializeProperties": true,
}

// extractReqArgs extracts request arguments from the provided bytecode.
func extractReqArgs(bytecode *Bytecode) map[string]interface{} {
	args := make(map[string]interface{})

	for _, insn := range bytecode.sourceInstructions {
		switch insn.operator {
		case "withStrategies":
			for k, v := range extractWithStrategiesReqArgs(insn) {
				args[k] = v
			}
		case "with":
			if k, v := extractWithReqArg(insn); k != "" {
				args[k] = v
			}
		}
	}

	return args
}

// extractWithStrategiesReqArgs extracts request arguments from the passed
// "withStrategies" source instruction. Only OptionsStrategy is considered.
func extractWithStrategiesReqArgs(insn instruction) map[string]interface{} {
	args := make(map[string]interface{})

	for _, strategyInterface := range insn.arguments {
		strategy, ok := strategyInterface.(*traversalStrategy)
		if !ok {
			// (*GraphTraversalSource).WithStrategies accepts
			// TraversalStrategy parameters only. Thus, this
			// should be unreachable.
			continue
		}

		if strategy.name != decorationNamespace+"OptionsStrategy" {
			continue
		}

		for k, v := range strategy.configuration {
			if allowedReqArgs[k] {
				args[k] = v
			}
		}
	}

	return args
}

// extractWithReqArg extracts a request argument from the passed "with" source
// instruction.
func extractWithReqArg(insn instruction) (key string, value interface{}) {
	if len(insn.arguments) != 2 {
		// (*GraphTraversalSource).With accepts two parameters. Thus,
		// this should be unreachable.
		return "", nil
	}

	key, ok := insn.arguments[0].(string)
	if !ok {
		return "", nil
	}

	if !allowedReqArgs[key] {
		return "", nil
	}

	return key, insn.arguments[1]
}
