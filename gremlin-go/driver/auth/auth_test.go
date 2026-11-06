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

package auth

import (
	"context"
	"encoding/base64"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"

	gremlingo "github.com/apache/tinkerpop/gremlin-go/v4/driver"
)

const graphBinaryMimeType = "application/vnd.graphbinary-v4.0"

func createMockRequest() *gremlingo.HttpRequest {
	req, _ := gremlingo.NewHttpRequest("POST", "https://test_url:8182/gremlin")
	req.Headers.Set("Content-Type", graphBinaryMimeType)
	req.Headers.Set("Accept", graphBinaryMimeType)
	req.Body = []byte(`{"gremlin":"g.V()"}`)
	return req
}

func TestBasic(t *testing.T) {
	t.Run("adds authorization header", func(t *testing.T) {
		req := createMockRequest()
		assert.Empty(t, req.Headers.Get(gremlingo.HeaderAuthorization))

		interceptor := Basic("username", "password")
		err := interceptor(req)

		assert.NoError(t, err)
		authHeader := req.Headers.Get(gremlingo.HeaderAuthorization)
		assert.True(t, strings.HasPrefix(authHeader, "Basic "))

		// Verify encoding
		encoded := strings.TrimPrefix(authHeader, "Basic ")
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		assert.NoError(t, err)
		assert.Equal(t, "username:password", string(decoded))
	})
}

// mockCredentialsProvider implements aws.CredentialsProvider for testing
type mockCredentialsProvider struct {
	accessKey    string
	secretKey    string
	sessionToken string
}

func (m *mockCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID:     m.accessKey,
		SecretAccessKey: m.secretKey,
		SessionToken:    m.sessionToken,
	}, nil
}

func TestSigV4(t *testing.T) {
	t.Run("adds signed headers", func(t *testing.T) {
		req := createMockRequest()
		assert.Empty(t, req.Headers.Get("Authorization"))
		assert.Empty(t, req.Headers.Get("X-Amz-Date"))

		provider := &mockCredentialsProvider{
			accessKey: "MOCK_ID",
			secretKey: "MOCK_KEY",
		}
		interceptor := SigV4WithCredentials("gremlin-east-1", "tinkerpop-sigv4", provider)
		err := interceptor(req)

		assert.NoError(t, err)
		assert.NotEmpty(t, req.Headers.Get("X-Amz-Date"))
		authHeader := req.Headers.Get("Authorization")
		assert.True(t, strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 Credential=MOCK_ID"))
		assert.Contains(t, authHeader, "gremlin-east-1/tinkerpop-sigv4/aws4_request")
		assert.Contains(t, authHeader, "Signature=")
	})

	t.Run("adds session token when provided", func(t *testing.T) {
		req := createMockRequest()
		assert.Empty(t, req.Headers.Get("X-Amz-Security-Token"))

		provider := &mockCredentialsProvider{
			accessKey:    "MOCK_ID",
			secretKey:    "MOCK_KEY",
			sessionToken: "MOCK_TOKEN",
		}
		interceptor := SigV4WithCredentials("gremlin-east-1", "tinkerpop-sigv4", provider)
		err := interceptor(req)

		assert.NoError(t, err)
		assert.Equal(t, "MOCK_TOKEN", req.Headers.Get("X-Amz-Security-Token"))
		authHeader := req.Headers.Get("Authorization")
		assert.True(t, strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 Credential="))
		assert.Contains(t, authHeader, "gremlin-east-1/tinkerpop-sigv4/aws4_request")
	})

	t.Run("auto-serializes *RequestMessage before signing", func(t *testing.T) {
		provider := &mockCredentialsProvider{
			accessKey: "MOCK_ID",
			secretKey: "MOCK_KEY",
		}
		interceptor := SigV4WithCredentials("gremlin-east-1", "tinkerpop-sigv4", provider)

		req, err := gremlingo.NewHttpRequest("POST", "https://test_url:8182/gremlin")
		assert.NoError(t, err)
		req.Headers.Set("Content-Type", "application/json")
		req.Headers.Set("Accept", graphBinaryMimeType)
		req.Body = &gremlingo.RequestMessage{Gremlin: "g.V()", Fields: map[string]interface{}{}}

		err = interceptor(req)
		assert.NoError(t, err)

		bodyBytes, ok := req.Body.([]byte)
		assert.True(t, ok, "Body should be []byte after auto-serialization")
		assert.NotEmpty(t, bodyBytes)
		assert.NotEmpty(t, req.Headers.Get("Authorization"))
		assert.NotEmpty(t, req.Headers.Get("X-Amz-Date"))
		assert.Contains(t, req.Headers.Get("Authorization"), "AWS4-HMAC-SHA256")
	})
}

// signedHeadersFromAuth extracts the SignedHeaders list from an Authorization header value.
func signedHeadersFromAuth(authHeader string) string {
	const marker = "SignedHeaders="
	idx := strings.Index(authHeader, marker)
	if idx < 0 {
		return ""
	}
	rest := authHeader[idx+len(marker):]
	if end := strings.Index(rest, ","); end >= 0 {
		return rest[:end]
	}
	return rest
}

// TestSigV4SignedHeaders pins the signed header set: only host and the headers the AWS SDK adds
// itself are signed, transport-managed headers such as accept-encoding are never signed even when
// present on the request, and the session token is signed only when session credentials are used.
func TestSigV4SignedHeaders(t *testing.T) {
	// A default-port (443) https URL so the test also covers host:port stripping: the signed Host
	// must be the bare hostname, matching what a spec-compliant verifier reconstructs.
	newRequest := func() *gremlingo.HttpRequest {
		req, err := gremlingo.NewHttpRequest("POST", "https://example.com:443/gremlin")
		assert.NoError(t, err)
		// Seed transport-managed and content headers that must NOT end up signed.
		req.Headers.Set("Accept", graphBinaryMimeType)
		req.Headers.Set("Content-Type", "application/json")
		req.Headers.Set("Accept-Encoding", "deflate")
		req.Headers.Set("User-Agent", "gremlin-go-test")
		req.Body = []byte(`{"gremlin":"g.V().count()"}`)
		return req
	}

	t.Run("basic credentials sign only host, date, content-sha256", func(t *testing.T) {
		req := newRequest()
		provider := &mockCredentialsProvider{accessKey: "MOCK_ID", secretKey: "MOCK_KEY"}
		err := SigV4WithCredentials("region-1", "example-service", provider)(req)
		assert.NoError(t, err)

		signedHeaders := signedHeadersFromAuth(req.Headers.Get("Authorization"))
		// aws-sdk-go-v2 leaves x-amz-content-sha256 in the signed set (see auth.go); that is the
		// SDK's natural behavior and is intentionally left as-is.
		assert.Equal(t, "host;x-amz-content-sha256;x-amz-date", signedHeaders)
		assert.NotContains(t, signedHeaders, "accept-encoding")
		assert.NotContains(t, signedHeaders, "content-type")
		assert.NotContains(t, signedHeaders, "x-amz-security-token")

		// The signed (and sent) Host must omit the default :443 port.
		assert.Equal(t, "example.com", req.URL.Host)
	})

	t.Run("session credentials also sign the security token", func(t *testing.T) {
		req := newRequest()
		provider := &mockCredentialsProvider{accessKey: "MOCK_ID", secretKey: "MOCK_KEY", sessionToken: "MOCK_TOKEN"}
		err := SigV4WithCredentials("region-1", "example-service", provider)(req)
		assert.NoError(t, err)

		signedHeaders := signedHeadersFromAuth(req.Headers.Get("Authorization"))
		assert.Equal(t, "host;x-amz-content-sha256;x-amz-date;x-amz-security-token", signedHeaders)
		assert.Equal(t, "MOCK_TOKEN", req.Headers.Get("X-Amz-Security-Token"))
	})
}

// TestSigV4HostPortStripping verifies that the default port is removed from the host for signing
// while non-default ports are preserved, and that IPv6 literals keep their brackets (stripping the
// port must not corrupt the host into an unparseable form).
func TestSigV4HostPortStripping(t *testing.T) {
	cases := []struct {
		url      string
		wantHost string
	}{
		{"https://example.com:443/gremlin", "example.com"},
		{"http://example.com:80/gremlin", "example.com"},
		{"https://example.com:8182/gremlin", "example.com:8182"},
		{"https://example.com/gremlin", "example.com"},
		{"https://[::1]:443/gremlin", "[::1]"},
		{"https://[2001:db8::1]:443/gremlin", "[2001:db8::1]"},
		{"https://[::1]:8182/gremlin", "[::1]:8182"},
	}
	for _, tc := range cases {
		t.Run(tc.url, func(t *testing.T) {
			req, err := gremlingo.NewHttpRequest("POST", tc.url)
			assert.NoError(t, err)
			req.Body = []byte(`{"gremlin":"g.V()"}`)
			provider := &mockCredentialsProvider{accessKey: "MOCK_ID", secretKey: "MOCK_KEY"}

			err = SigV4WithCredentials("region-1", "example-service", provider)(req)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantHost, req.URL.Host)
			// The mutated URL must remain parseable (an IPv6 host that lost its brackets would
			// fail to re-parse, breaking the wire request built from req.URL.String()).
			_, parseErr := url.Parse(req.URL.String())
			assert.NoError(t, parseErr)
		})
	}
}
