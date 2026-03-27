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

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInterceptorReceivesRawRequest verifies that interceptors receive the raw *RequestMessage
// object in HttpRequest.Body, not serialized []byte.
func TestInterceptorReceivesRawRequest(t *testing.T) {
	// Mock server that accepts the request (we don't care about the response for this test)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create connection with non-nil serializer (default behavior of newConnection)
	conn := newConnection(newTestLogHandler(), server.URL, &connectionSettings{})

	var capturedBodyType reflect.Type
	var capturedBody interface{}

	conn.AddInterceptor(func(req *HttpRequest) error {
		capturedBody = req.Body
		capturedBodyType = reflect.TypeOf(req.Body)
		return nil
	})

	// Submit a request with a known gremlin query
	rs, err := conn.submit(&RequestMessage{Gremlin: "g.V().count()", Fields: map[string]interface{}{}})
	require.NoError(t, err)
	_, _ = rs.All() // drain result set

	assert.Equal(t, reflect.TypeOf((*RequestMessage)(nil)), capturedBodyType,
		"interceptor should receive *RequestMessage in Body, got %v", capturedBodyType)

	r, typeAssertOk := capturedBody.(*RequestMessage)
	assert.True(t, typeAssertOk, "interceptor should be able to type-assert Body to *RequestMessage")
	if typeAssertOk {
		assert.Equal(t, "g.V().count()", r.Gremlin,
			"interceptor should be able to read the Gremlin field from the raw request")
	}
}

// TestSigV4AuthWithSerializeInterceptor verifies that SerializeRequest() + SigV4Auth
// works in a chain. SerializeRequest converts *RequestMessage to []byte, then SigV4Auth
// can sign the serialized body.
func TestSigV4AuthWithSerializeInterceptor(t *testing.T) {
	var capturedHeaders http.Header
	var capturedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()
		body, err := io.ReadAll(r.Body)
		if err == nil {
			capturedBody = body
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := newConnection(newTestLogHandler(), server.URL, &connectionSettings{})

	mockProvider := &mockCredentialsProvider{
		accessKey: "MOCK_ID",
		secretKey: "MOCK_KEY",
	}

	conn.AddInterceptor(SerializeRequest())
	conn.AddInterceptor(SigV4AuthWithCredentials("gremlin-east-1", "tinkerpop-sigv4", mockProvider))

	rs, err := conn.submit(&RequestMessage{Gremlin: "g.V().count()", Fields: map[string]interface{}{}})
	require.NoError(t, err)
	_, _ = rs.All() // drain

	// SigV4 should have added Authorization and X-Amz-Date headers
	assert.NotEmpty(t, capturedHeaders.Get("Authorization"),
		"SigV4Auth should set Authorization header after SerializeRequest")
	assert.NotEmpty(t, capturedHeaders.Get("X-Amz-Date"),
		"SigV4Auth should set X-Amz-Date header")
	assert.Contains(t, capturedHeaders.Get("Authorization"), "AWS4-HMAC-SHA256",
		"Authorization header should use AWS4-HMAC-SHA256 signing algorithm")

	// Body should be valid serialized bytes
	assert.NotEmpty(t, capturedBody, "body should be non-empty serialized bytes")
	assert.Equal(t, byte(0x84), capturedBody[0],
		"body should start with GraphBinary version byte 0x84")
}

// TestSigV4Auth_AutoSerializesInChain verifies that SigV4Auth works as the only
// interceptor — it auto-serializes *RequestMessage before signing.
func TestSigV4Auth_AutoSerializesInChain(t *testing.T) {
	var capturedHeaders http.Header
	var capturedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()
		body, err := io.ReadAll(r.Body)
		if err == nil {
			capturedBody = body
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := newConnection(newTestLogHandler(), server.URL, &connectionSettings{})

	mockProvider := &mockCredentialsProvider{
		accessKey: "MOCK_ID",
		secretKey: "MOCK_KEY",
	}

	// Only SigV4Auth — no SerializeRequest() needed
	conn.AddInterceptor(SigV4AuthWithCredentials("gremlin-east-1", "tinkerpop-sigv4", mockProvider))

	rs, err := conn.submit(&RequestMessage{Gremlin: "g.V().count()", Fields: map[string]interface{}{}})
	require.NoError(t, err)
	_, _ = rs.All()

	assert.NotEmpty(t, capturedHeaders.Get("Authorization"),
		"SigV4Auth should set Authorization header")
	assert.Contains(t, capturedHeaders.Get("Authorization"), "AWS4-HMAC-SHA256")
	assert.NotEmpty(t, capturedBody, "body should be non-empty serialized bytes")
	assert.Equal(t, byte(0x84), capturedBody[0],
		"body should start with GraphBinary version byte 0x84")
}

// TestMultipleInterceptors_SerializeThenAuth verifies that a custom interceptor can
// modify the raw request, then SerializeRequest serializes it, then BasicAuth adds headers.
func TestMultipleInterceptors_SerializeThenAuth(t *testing.T) {
	var capturedAuthHeader string
	var capturedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuthHeader = r.Header.Get("Authorization")
		body, err := io.ReadAll(r.Body)
		if err == nil {
			capturedBody = body
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := newConnection(newTestLogHandler(), server.URL, &connectionSettings{})

	// Custom interceptor that modifies the raw request fields
	conn.AddInterceptor(func(req *HttpRequest) error {
		r, ok := req.Body.(*RequestMessage)
		if !ok {
			return fmt.Errorf("expected *RequestMessage, got %T", req.Body)
		}
		// Add a custom field to the request
		r.Fields["customField"] = "customValue"
		return nil
	})

	// SerializeRequest converts the modified *RequestMessage to []byte
	conn.AddInterceptor(SerializeRequest())

	// BasicAuth adds the Authorization header (works on any body type)
	conn.AddInterceptor(BasicAuth("admin", "secret"))

	rs, err := conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}})
	require.NoError(t, err)
	_, _ = rs.All() // drain

	// BasicAuth should have set the Authorization header
	assert.Equal(t, "Basic YWRtaW46c2VjcmV0", capturedAuthHeader,
		"Authorization header should be Basic base64(admin:secret)")

	// Body should be valid serialized bytes (from SerializeRequest)
	assert.NotEmpty(t, capturedBody, "body should be non-empty serialized bytes")
	assert.Equal(t, byte(0x84), capturedBody[0],
		"body should start with GraphBinary version byte 0x84")
}

// TestInterceptor_IoReaderBody verifies that an interceptor can set Body to an io.Reader
// and the request is sent correctly.
func TestInterceptor_IoReaderBody(t *testing.T) {
	var capturedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err == nil {
			capturedBody = body
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := newConnection(newTestLogHandler(), server.URL, &connectionSettings{})

	customPayload := []byte("custom binary payload")

	// Interceptor replaces Body with an io.Reader
	conn.AddInterceptor(func(req *HttpRequest) error {
		req.Body = bytes.NewReader(customPayload)
		return nil
	})

	rs, err := conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}})
	require.NoError(t, err)
	_, _ = rs.All() // drain

	// The server should receive the custom payload from the io.Reader
	assert.Equal(t, customPayload, capturedBody,
		"server should receive the custom payload set via io.Reader")
}

// TestInterceptor_NilSerializerNoSerialization verifies that when serializer is nil
// and no interceptor serializes, the correct error message is produced.
func TestInterceptor_NilSerializerNoSerialization(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := newConnection(newTestLogHandler(), server.URL, &connectionSettings{})
	conn.serializer = nil // explicitly nil serializer

	rs, err := conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}})
	require.NoError(t, err)

	_, _ = rs.All() // drain — this triggers the async executeAndStream
	rsErr := rs.GetError()
	require.Error(t, rsErr, "should get an error when serializer is nil and no interceptor serializes")
	assert.Contains(t, rsErr.Error(), "request body was not serialized",
		"error message should indicate the body was not serialized")
}

// TestInterceptor_HttpRequestBody verifies that an interceptor can set Body to *http.Request
// and the driver sends it directly, using the *http.Request's headers and body instead of
// HttpRequest.Headers.
func TestInterceptor_HttpRequestBody(t *testing.T) {
	var capturedHeaders http.Header
	var capturedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()
		body, err := io.ReadAll(r.Body)
		if err == nil {
			capturedBody = body
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := newConnection(newTestLogHandler(), server.URL, &connectionSettings{})

	customBody := []byte("custom-http-request-body")

	// Interceptor builds a complete *http.Request and sets it as Body
	conn.AddInterceptor(func(req *HttpRequest) error {
		httpGoReq, err := http.NewRequest(http.MethodPost, req.URL.String(), bytes.NewReader(customBody))
		if err != nil {
			return err
		}
		httpGoReq.Header.Set("X-Custom-Header", "custom-value")
		httpGoReq.Header.Set("Content-Type", "application/octet-stream")
		req.Body = httpGoReq
		return nil
	})

	// Also set a header on HttpRequest.Headers that should NOT appear,
	// because *http.Request body bypasses HttpRequest.Headers
	conn.AddInterceptor(func(req *HttpRequest) error {
		req.Headers.Set("X-Should-Not-Appear", "ignored")
		return nil
	})

	rs, err := conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}})
	require.NoError(t, err)
	_, _ = rs.All() // drain

	// The server should receive headers from the *http.Request, not from HttpRequest.Headers
	assert.Equal(t, "custom-value", capturedHeaders.Get("X-Custom-Header"),
		"server should receive custom header from *http.Request")
	assert.Equal(t, "application/octet-stream", capturedHeaders.Get("Content-Type"),
		"server should receive Content-Type from *http.Request")
	assert.Empty(t, capturedHeaders.Get("X-Should-Not-Appear"),
		"headers set on HttpRequest.Headers should not appear when Body is *http.Request")

	// The server should receive the body from the *http.Request
	assert.Equal(t, customBody, capturedBody,
		"server should receive body from the *http.Request")
}

// TestInterceptor_ErrorPropagation verifies that when an interceptor returns an error,
// it is propagated to the ResultSet.
func TestInterceptor_ErrorPropagation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := newConnection(newTestLogHandler(), server.URL, &connectionSettings{})

	conn.AddInterceptor(func(req *HttpRequest) error {
		return fmt.Errorf("interceptor failed")
	})

	rs, err := conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}})
	require.NoError(t, err)

	_, _ = rs.All() // drain — triggers async executeAndStream
	rsErr := rs.GetError()
	require.Error(t, rsErr, "interceptor error should propagate to ResultSet")
	assert.Contains(t, rsErr.Error(), "interceptor failed",
		"ResultSet error should contain the interceptor's error message")
}

// TestInterceptor_UnsupportedBodyType verifies that setting Body to an unsupported type
// (e.g., an int) produces the "unsupported body type" error.
func TestInterceptor_UnsupportedBodyType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := newConnection(newTestLogHandler(), server.URL, &connectionSettings{})

	// Interceptor sets Body to an unsupported type
	conn.AddInterceptor(func(req *HttpRequest) error {
		req.Body = 42
		return nil
	})

	rs, err := conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}})
	require.NoError(t, err)

	_, _ = rs.All() // drain
	rsErr := rs.GetError()
	require.Error(t, rsErr, "unsupported body type should produce an error")
	assert.Contains(t, rsErr.Error(), "unsupported body type",
		"error message should indicate unsupported body type")
}

// TestInterceptor_ChainOrder verifies that interceptors run in the order they are added.
func TestInterceptor_ChainOrder(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := newConnection(newTestLogHandler(), server.URL, &connectionSettings{})

	var order []int

	conn.AddInterceptor(func(req *HttpRequest) error {
		order = append(order, 1)
		return nil
	})
	conn.AddInterceptor(func(req *HttpRequest) error {
		order = append(order, 2)
		return nil
	})
	conn.AddInterceptor(func(req *HttpRequest) error {
		order = append(order, 3)
		return nil
	})

	rs, err := conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}})
	require.NoError(t, err)
	_, _ = rs.All() // drain

	assert.Equal(t, []int{1, 2, 3}, order,
		"interceptors should run in the order they were added")
}

// TestSigV4Auth_RejectsNonByteBody verifies that SigV4Auth returns an error when Body
// is not []byte and not *RequestMessage (e.g., an io.Reader).
func TestSigV4Auth_RejectsNonByteBody(t *testing.T) {
	provider := &mockCredentialsProvider{
		accessKey: "MOCK_ID",
		secretKey: "MOCK_KEY",
	}
	interceptor := SigV4AuthWithCredentials("gremlin-east-1", "tinkerpop-sigv4", provider)

	req, err := NewHttpRequest("POST", "https://test_url:8182/gremlin")
	require.NoError(t, err)
	req.Headers.Set("Content-Type", graphBinaryMimeType)
	req.Headers.Set("Accept", graphBinaryMimeType)

	// Set Body to an unsupported type (not []byte and not *RequestMessage)
	req.Body = strings.NewReader("not bytes")

	err = interceptor(req)
	require.Error(t, err, "SigV4Auth should reject non-[]byte, non-*RequestMessage body")
	assert.Contains(t, err.Error(), "SigV4 signing requires body to be []byte",
		"error message should indicate SigV4 requires []byte body")
}

// TestSigV4Auth_AutoSerializesRequestMessage verifies that SigV4Auth automatically
// serializes *RequestMessage to []byte before signing.
func TestSigV4Auth_AutoSerializesRequestMessage(t *testing.T) {
	provider := &mockCredentialsProvider{
		accessKey: "MOCK_ID",
		secretKey: "MOCK_KEY",
	}
	interceptor := SigV4AuthWithCredentials("gremlin-east-1", "tinkerpop-sigv4", provider)

	req, err := NewHttpRequest("POST", "https://test_url:8182/gremlin")
	require.NoError(t, err)
	req.Headers.Set("Content-Type", graphBinaryMimeType)
	req.Headers.Set("Accept", graphBinaryMimeType)

	// Set Body to *RequestMessage — SigV4Auth should auto-serialize it
	req.Body = &RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}}

	err = interceptor(req)
	require.NoError(t, err, "SigV4Auth should auto-serialize *RequestMessage")

	// Body should now be []byte (serialized)
	bodyBytes, ok := req.Body.([]byte)
	assert.True(t, ok, "Body should be []byte after SigV4Auth auto-serialization")
	assert.NotEmpty(t, bodyBytes, "serialized body should be non-empty")

	// SigV4 headers should be set
	assert.NotEmpty(t, req.Headers.Get("Authorization"), "Authorization header should be set")
	assert.NotEmpty(t, req.Headers.Get("X-Amz-Date"), "X-Amz-Date header should be set")
	assert.Contains(t, req.Headers.Get("Authorization"), "AWS4-HMAC-SHA256")
}
