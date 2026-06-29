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
