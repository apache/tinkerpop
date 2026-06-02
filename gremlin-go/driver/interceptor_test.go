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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
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

// TestInterceptorCanModifyHeaders verifies that interceptors can read and modify headers.
func TestInterceptorCanModifyHeaders(t *testing.T) {
	var capturedHeaders http.Header

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := newConnection(newTestLogHandler(), server.URL, &connectionSettings{})

	conn.AddInterceptor(func(req *HttpRequest) error {
		req.Headers.Set("X-Custom-Header", "custom-value")
		return nil
	})

	rs, err := conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}})
	require.NoError(t, err)
	_, _ = rs.All()

	assert.Equal(t, "custom-value", capturedHeaders.Get("X-Custom-Header"),
		"interceptor should be able to set custom headers")
}

// TestInterceptorCanModifyURI verifies that interceptors can modify the request URI.
func TestInterceptorCanModifyURI(t *testing.T) {
	var capturedPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := newConnection(newTestLogHandler(), server.URL+"/gremlin", &connectionSettings{})

	conn.AddInterceptor(func(req *HttpRequest) error {
		req.URL.Path = "/custom-path"
		return nil
	})

	rs, err := conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}})
	require.NoError(t, err)
	_, _ = rs.All()

	assert.Equal(t, "/custom-path", capturedPath,
		"interceptor should be able to modify the URL path")
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
	_, _ = rs.All()

	assert.Equal(t, []int{1, 2, 3}, order,
		"interceptors should run in the order they were added")
}

// TestSerializeBody_JSONOutput verifies that SerializeBody produces valid JSON containing
// the gremlin field and all fields from RequestMessage.Fields.
func TestSerializeBody_JSONOutput(t *testing.T) {
	req, err := NewHttpRequest("POST", "http://localhost:8182/gremlin")
	require.NoError(t, err)

	req.Body = &RequestMessage{
		Gremlin: "g.V().has('name','marko')",
		Fields:  map[string]interface{}{"language": "gremlin-lang", "g": "g"},
	}

	data, err := req.SerializeBody()
	require.NoError(t, err)

	var parsed map[string]interface{}
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err, "SerializeBody should produce valid JSON")

	assert.Equal(t, "g.V().has('name','marko')", parsed["gremlin"])
	assert.Equal(t, "gremlin-lang", parsed["language"])
	assert.Equal(t, "g", parsed["g"])
}

// TestSerializeBody_SetsContentTypeHeader verifies that SerializeBody sets Content-Type to application/json.
func TestSerializeBody_SetsContentTypeHeader(t *testing.T) {
	req, err := NewHttpRequest("POST", "http://localhost:8182/gremlin")
	require.NoError(t, err)

	req.Body = &RequestMessage{
		Gremlin: "g.V()",
		Fields:  map[string]interface{}{},
	}

	_, err = req.SerializeBody()
	require.NoError(t, err)

	assert.Equal(t, "application/json", req.Headers.Get(HeaderContentType),
		"SerializeBody should set Content-Type to application/json")
}

// TestSerializeBody_SetsContentLengthHeader verifies that SerializeBody sets Content-Length
// to the byte length of the serialized body.
func TestSerializeBody_SetsContentLengthHeader(t *testing.T) {
	req, err := NewHttpRequest("POST", "http://localhost:8182/gremlin")
	require.NoError(t, err)

	req.Body = &RequestMessage{
		Gremlin: "g.V()",
		Fields:  map[string]interface{}{},
	}

	data, err := req.SerializeBody()
	require.NoError(t, err)

	expected := strconv.Itoa(len(data))
	assert.Equal(t, expected, req.Headers.Get("Content-Length"),
		"SerializeBody should set Content-Length to byte length of the body")
}

// TestSerializeBody_Idempotent verifies that calling SerializeBody when body is already
// []byte returns the same bytes without re-serialization.
func TestSerializeBody_Idempotent(t *testing.T) {
	req, err := NewHttpRequest("POST", "http://localhost:8182/gremlin")
	require.NoError(t, err)

	req.Body = &RequestMessage{
		Gremlin: "g.V()",
		Fields:  map[string]interface{}{"g": "g"},
	}

	// First call serializes
	data1, err := req.SerializeBody()
	require.NoError(t, err)

	// Second call should return same bytes
	data2, err := req.SerializeBody()
	require.NoError(t, err)

	assert.Equal(t, data1, data2,
		"SerializeBody should be idempotent, returning the same bytes on subsequent calls")
}

// TestSerializeBody_MultipleCalls verifies that multiple calls produce identical results.
func TestSerializeBody_MultipleCalls(t *testing.T) {
	req, err := NewHttpRequest("POST", "http://localhost:8182/gremlin")
	require.NoError(t, err)

	req.Body = &RequestMessage{
		Gremlin: "g.V().count()",
		Fields:  map[string]interface{}{"language": "gremlin-lang"},
	}

	results := make([][]byte, 3)
	for i := range results {
		data, err := req.SerializeBody()
		require.NoError(t, err)
		results[i] = data
	}

	assert.Equal(t, results[0], results[1])
	assert.Equal(t, results[1], results[2])
}

// TestSerializeBody_UnsupportedType verifies that SerializeBody returns an error when
// body is neither *RequestMessage nor []byte.
func TestSerializeBody_UnsupportedType(t *testing.T) {
	req, err := NewHttpRequest("POST", "http://localhost:8182/gremlin")
	require.NoError(t, err)

	req.Body = 42

	_, err = req.SerializeBody()
	require.Error(t, err, "SerializeBody should return error for unsupported body type")
	assert.Contains(t, err.Error(), "cannot serialize request body")
}

// TestFieldMutationBeforeSerialization verifies that an interceptor can modify
// RequestMessage fields and the serialized output reflects those changes.
func TestFieldMutationBeforeSerialization(t *testing.T) {
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

	// Interceptor that adds a custom field before serialization
	conn.AddInterceptor(func(req *HttpRequest) error {
		r, ok := req.Body.(*RequestMessage)
		if !ok {
			return fmt.Errorf("expected *RequestMessage, got %T", req.Body)
		}
		r.Fields["customField"] = "customValue"
		return nil
	})

	rs, err := conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{"g": "g"}})
	require.NoError(t, err)
	_, _ = rs.All()

	// Parse the JSON body sent to the server
	var parsed map[string]interface{}
	err = json.Unmarshal(capturedBody, &parsed)
	require.NoError(t, err, "server should receive valid JSON")

	assert.Equal(t, "g.V()", parsed["gremlin"])
	assert.Equal(t, "g", parsed["g"])
	assert.Equal(t, "customValue", parsed["customField"],
		"interceptor field mutation should be reflected in the serialized output")
}

// TestSigV4AuthWithSerializeBody verifies that SigV4Auth calls SerializeBody and signs
// the request properly.
func TestSigV4AuthWithSerializeBody(t *testing.T) {
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

	// SigV4 should have added Authorization and X-Amz-Date headers
	assert.NotEmpty(t, capturedHeaders.Get("Authorization"),
		"SigV4Auth should set Authorization header")
	assert.NotEmpty(t, capturedHeaders.Get("X-Amz-Date"),
		"SigV4Auth should set X-Amz-Date header")
	assert.Contains(t, capturedHeaders.Get("Authorization"), "AWS4-HMAC-SHA256",
		"Authorization header should use AWS4-HMAC-SHA256 signing algorithm")

	// Body should be valid JSON
	assert.NotEmpty(t, capturedBody, "body should be non-empty serialized bytes")
	var parsed map[string]interface{}
	err = json.Unmarshal(capturedBody, &parsed)
	require.NoError(t, err, "body should be valid JSON")
	assert.Equal(t, "g.V().count()", parsed["gremlin"])
}

// TestSigV4Auth_AutoSerializesRequestMessage verifies that SigV4Auth automatically
// serializes *RequestMessage to JSON bytes before signing.
func TestSigV4Auth_AutoSerializesRequestMessage(t *testing.T) {
	provider := &mockCredentialsProvider{
		accessKey: "MOCK_ID",
		secretKey: "MOCK_KEY",
	}
	interceptor := SigV4AuthWithCredentials("gremlin-east-1", "tinkerpop-sigv4", provider)

	req, err := NewHttpRequest("POST", "https://test_url:8182/gremlin")
	require.NoError(t, err)
	req.Headers.Set("Content-Type", "application/json")
	req.Headers.Set("Accept", graphBinaryMimeType)

	// Set Body to *RequestMessage
	req.Body = &RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}}

	err = interceptor(req)
	require.NoError(t, err, "SigV4Auth should auto-serialize *RequestMessage")

	// Body should now be []byte (serialized JSON)
	bodyBytes, ok := req.Body.([]byte)
	assert.True(t, ok, "Body should be []byte after SigV4Auth auto-serialization")
	assert.NotEmpty(t, bodyBytes, "serialized body should be non-empty")

	// Verify it's valid JSON
	var parsed map[string]interface{}
	err = json.Unmarshal(bodyBytes, &parsed)
	require.NoError(t, err, "body should be valid JSON after auto-serialization")
	assert.Equal(t, "g.V()", parsed["gremlin"])

	// SigV4 headers should be set
	assert.NotEmpty(t, req.Headers.Get("Authorization"), "Authorization header should be set")
	assert.NotEmpty(t, req.Headers.Get("X-Amz-Date"), "X-Amz-Date header should be set")
	assert.Contains(t, req.Headers.Get("Authorization"), "AWS4-HMAC-SHA256")
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
	req.Headers.Set("Content-Type", "application/json")
	req.Headers.Set("Accept", graphBinaryMimeType)

	// Set Body to an unsupported type (not []byte and not *RequestMessage)
	req.Body = bytes.NewReader([]byte("not bytes"))

	err = interceptor(req)
	require.Error(t, err, "SigV4Auth should reject non-[]byte, non-*RequestMessage body")
	assert.Contains(t, err.Error(), "cannot serialize request body",
		"error message should indicate unsupported body type")
}

// TestMultipleInterceptors_MutateThenAuth verifies that a custom interceptor can
// modify the raw request fields, then BasicAuth adds headers, and the driver
// auto-serializes to JSON.
func TestMultipleInterceptors_MutateThenAuth(t *testing.T) {
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

	// BasicAuth adds the Authorization header (works on any body type)
	conn.AddInterceptor(BasicAuth("admin", "secret"))

	rs, err := conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}})
	require.NoError(t, err)
	_, _ = rs.All() // drain

	// BasicAuth should have set the Authorization header
	assert.Equal(t, "Basic YWRtaW46c2VjcmV0", capturedAuthHeader,
		"Authorization header should be Basic base64(admin:secret)")

	// Body should be valid JSON (auto-serialized by driver after interceptors)
	assert.NotEmpty(t, capturedBody, "body should be non-empty")
	var parsed map[string]interface{}
	err = json.Unmarshal(capturedBody, &parsed)
	require.NoError(t, err, "body should be valid JSON")
	assert.Equal(t, "g.V()", parsed["gremlin"])
	assert.Equal(t, "customValue", parsed["customField"])
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

	rs, err := conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}})
	require.NoError(t, err)
	_, _ = rs.All() // drain

	assert.Equal(t, "custom-value", capturedHeaders.Get("X-Custom-Header"))
	assert.Equal(t, "application/octet-stream", capturedHeaders.Get("Content-Type"))
	assert.Equal(t, customBody, capturedBody)
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

	_, err := conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}})
	require.Error(t, err, "interceptor error should propagate")
	assert.Contains(t, err.Error(), "interceptor failed",
		"error should contain the interceptor's error message")
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

	_, err := conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}})
	require.Error(t, err, "unsupported body type should produce an error")
	assert.Contains(t, err.Error(), "unsupported body type",
		"error message should indicate unsupported body type")
}

// TestDriverAutoSerializes verifies that without any interceptors, the driver
// auto-serializes the request body to JSON.
func TestDriverAutoSerializes(t *testing.T) {
	var capturedBody []byte
	var capturedContentType string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedContentType = r.Header.Get("Content-Type")
		body, err := io.ReadAll(r.Body)
		if err == nil {
			capturedBody = body
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := newConnection(newTestLogHandler(), server.URL, &connectionSettings{})

	// No interceptors, driver should auto-serialize
	rs, err := conn.submit(&RequestMessage{
		Gremlin: "g.V().count()",
		Fields:  map[string]interface{}{"language": "gremlin-lang", "g": "g"},
	})
	require.NoError(t, err)
	_, _ = rs.All()

	assert.Equal(t, "application/json", capturedContentType,
		"Content-Type should be application/json")

	var parsed map[string]interface{}
	err = json.Unmarshal(capturedBody, &parsed)
	require.NoError(t, err, "body should be valid JSON")
	assert.Equal(t, "g.V().count()", parsed["gremlin"])
	assert.Equal(t, "gremlin-lang", parsed["language"])
	assert.Equal(t, "g", parsed["g"])
}

// TestAuthInterceptorIsAlwaysLast verifies that the Auth field is always appended
// to the end of the interceptor chain, regardless of configuration order.
func TestAuthInterceptorIsAlwaysLast(t *testing.T) {
	var order []int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Run("Auth runs after RequestInterceptors on Client", func(t *testing.T) {
		order = nil
		client, err := NewClient(server.URL,
			func(settings *ClientSettings) {
				settings.Auth = func(req *HttpRequest) error { order = append(order, 3); return nil }
				settings.RequestInterceptors = []RequestInterceptor{
					func(req *HttpRequest) error { order = append(order, 1); return nil },
					func(req *HttpRequest) error { order = append(order, 2); return nil },
				}
			})
		require.NoError(t, err)
		defer client.Close()

		rs, err := client.conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}})
		require.NoError(t, err)
		_, _ = rs.All()

		assert.Equal(t, []int{1, 2, 3}, order,
			"Auth interceptor should always run last")
	})

	t.Run("Auth runs after RequestInterceptors on DriverRemoteConnection", func(t *testing.T) {
		order = nil
		remote, err := NewDriverRemoteConnection(server.URL,
			func(settings *DriverRemoteConnectionSettings) {
				settings.Auth = func(req *HttpRequest) error { order = append(order, 3); return nil }
				settings.RequestInterceptors = []RequestInterceptor{
					func(req *HttpRequest) error { order = append(order, 1); return nil },
					func(req *HttpRequest) error { order = append(order, 2); return nil },
				}
			})
		require.NoError(t, err)
		defer remote.Close()

		rs, err := remote.client.conn.submit(&RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}})
		require.NoError(t, err)
		_, _ = rs.All()

		assert.Equal(t, []int{1, 2, 3}, order,
			"Auth interceptor should always run last")
	})
}
