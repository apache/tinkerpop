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

import "sync"

// SerializeRequest serializes a complete request with bytecode into GraphBinary format.
// This function enables developers to implement alternative transport protocols (e.g., gRPC, HTTP/2)
// while maintaining full compatibility with the Gremlin traversal API.
//
// The function creates a proper Gremlin request that includes the bytecode, serializes it using
// the GraphBinary serializer, and returns the raw bytes that can be transmitted over any transport.
//
// Parameters:
//   - bytecode: The Gremlin bytecode to serialize
//   - traversalSource: The name of the traversal source (typically "g")
//   - sessionId: The session ID for stateful sessions (empty string for stateless)
//
// Returns:
//   - []byte: The serialized request in GraphBinary format
//   - error: Any error encountered during serialization
//
// Example usage:
//
//	bytecode := &Bytecode{...}
//	bytes, err := SerializeRequest(bytecode, "g", "")
//	if err != nil {
//	    return err
//	}
//	// Send bytes over custom transport (gRPC, HTTP/2, etc.)
func SerializeRequest(bytecode *Bytecode, traversalSource, sessionId string) ([]byte, error) {
	// Use the existing makeBytecodeRequest function from request.go
	request := makeBytecodeRequest(bytecode, traversalSource, sessionId)

	// Serialize the request using GraphBinary serializer
	serializer := graphBinarySerializer{}
	return serializer.serializeMessage(&request)
}

// SerializeBytecode serializes bytecode into GraphBinary format using the default traversal source "g".
// This is a convenience wrapper around SerializeRequest for backward compatibility.
//
// Parameters:
//   - bytecode: The Gremlin bytecode to serialize
//
// Returns:
//   - []byte: The serialized request in GraphBinary format
//   - error: Any error encountered during serialization
func SerializeBytecode(bytecode *Bytecode) ([]byte, error) {
	return SerializeRequest(bytecode, "g", "")
}

// SerializeStringQuery serializes a string-based Gremlin query into GraphBinary format.
// This function enables sending raw Gremlin query strings over custom transport protocols.
//
// The function creates a proper Gremlin request that includes the query string, serializes it
// using the GraphBinary serializer, and returns the raw bytes.
//
// Parameters:
//   - query: The Gremlin query string to serialize
//   - traversalSource: The name of the traversal source (typically "g")
//   - sessionId: The session ID for stateful sessions (empty string for stateless)
//   - requestOptions: Options for the request (bindings, timeout, etc.)
//
// Returns:
//   - []byte: The serialized request in GraphBinary format
//   - error: Any error encountered during serialization
//
// Example usage:
//
//	bytes, err := SerializeStringQuery("g.V().count()", "g", "", RequestOptions{})
//	if err != nil {
//	    return err
//	}
//	// Send bytes over custom transport
func SerializeStringQuery(query string, traversalSource string, sessionId string, requestOptions RequestOptions) ([]byte, error) {
	request := makeStringRequest(query, traversalSource, sessionId, requestOptions)
	serializer := graphBinarySerializer{}
	return serializer.serializeMessage(&request)
}

// DeserializeResult deserializes a response message from GraphBinary format into a Result.
// This function enables receiving and processing responses from custom transport protocols.
//
// The function takes raw bytes received from a transport, deserializes them using the GraphBinary
// deserializer, and returns a Result object containing the response data.
//
// Parameters:
//   - data: The response bytes in GraphBinary format
//
// Returns:
//   - *Result: The deserialized result containing response data
//   - error: Any error encountered during deserialization
//
// Example usage:
//
//	// Receive bytes from custom transport
//	result, err := DeserializeResult(responseBytes)
//	if err != nil {
//	    return err
//	}
//	value := result.Data
func DeserializeResult(data []byte) (*Result, error) {
	serializer := graphBinarySerializer{}
	resp, err := serializer.deserializeMessage(data)
	if err != nil {
		return nil, err
	}

	result := &Result{
		Data: resp.responseResult.data,
	}
	return result, nil
}

// NewResultSet creates a new ResultSet from a slice of Result objects.
// This function enables custom transport implementations to create ResultSets from
// results collected via alternative protocols.
//
// The function creates a channel-based ResultSet, pre-populates it with the provided results,
// and closes the channel to indicate completion.
//
// Parameters:
//   - results: A slice of Result objects to include in the ResultSet
//
// Returns:
//   - ResultSet: A ResultSet containing all the provided results
//
// Example usage:
//
//	var results []*Result
//	// Collect results from custom transport
//	for _, responseBytes := range responses {
//	    result, _ := DeserializeResult(responseBytes)
//	    results = append(results, result)
//	}
//	resultSet := NewResultSet(results)
//	allResults, _ := resultSet.All()
func NewResultSet(results []*Result) ResultSet {
	// Create a channel-based result set with capacity for all results
	channelSize := len(results)
	if channelSize == 0 {
		channelSize = 1 // Ensure at least size 1
	}
	rs := newChannelResultSetCapacity("", &synchronizedMap{make(map[string]ResultSet), sync.Mutex{}}, channelSize).(*channelResultSet)

	// Add all results to the channel
	for _, result := range results {
		rs.channel <- result
	}

	// Close the channel to indicate no more results
	rs.channelMutex.Lock()
	rs.closed = true
	close(rs.channel)
	rs.channelMutex.Unlock()

	return rs
}
