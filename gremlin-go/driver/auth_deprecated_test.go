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
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
)

const deprecatedAuthMimeType = "application/vnd.graphbinary-v4.0"

func newDeprecatedAuthRequest(t *testing.T) *HttpRequest {
	req, err := NewHttpRequest("POST", "https://test_url:8182/gremlin")
	assert.NoError(t, err)
	req.Headers.Set("Content-Type", deprecatedAuthMimeType)
	req.Headers.Set("Accept", deprecatedAuthMimeType)
	req.Body = []byte(`{"gremlin":"g.V()"}`)
	return req
}

// mockAuthCredentialsProvider implements aws.CredentialsProvider for testing.
type mockAuthCredentialsProvider struct {
	accessKey    string
	secretKey    string
	sessionToken string
}

func (m *mockAuthCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID:     m.accessKey,
		SecretAccessKey: m.secretKey,
		SessionToken:    m.sessionToken,
	}, nil
}

// TestDeprecatedBasicAuth verifies the deprecated BasicAuth delegator produces a
// working interceptor that sets the Authorization header, equivalent to auth.Basic.
func TestDeprecatedBasicAuth(t *testing.T) {
	t.Run("adds authorization header", func(t *testing.T) {
		req := newDeprecatedAuthRequest(t)
		assert.Empty(t, req.Headers.Get(HeaderAuthorization))

		interceptor := BasicAuth("username", "password")
		err := interceptor(req)

		assert.NoError(t, err)
		authHeader := req.Headers.Get(HeaderAuthorization)
		assert.True(t, strings.HasPrefix(authHeader, "Basic "))

		encoded := strings.TrimPrefix(authHeader, "Basic ")
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		assert.NoError(t, err)
		assert.Equal(t, "username:password", string(decoded))
	})

	t.Run("matches expected encoding", func(t *testing.T) {
		req := newDeprecatedAuthRequest(t)
		err := BasicAuth("user", "pass")(req)
		assert.NoError(t, err)

		expected := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass"))
		assert.Equal(t, expected, req.Headers.Get(HeaderAuthorization))
	})
}

// TestDeprecatedSigV4Auth verifies the deprecated SigV4 delegators produce working
// interceptors that sign requests, equivalent to auth.SigV4WithCredentials.
func TestDeprecatedSigV4Auth(t *testing.T) {
	t.Run("adds signed headers", func(t *testing.T) {
		req := newDeprecatedAuthRequest(t)
		assert.Empty(t, req.Headers.Get("Authorization"))
		assert.Empty(t, req.Headers.Get("X-Amz-Date"))

		provider := &mockAuthCredentialsProvider{
			accessKey: "MOCK_ID",
			secretKey: "MOCK_KEY",
		}
		interceptor := SigV4AuthWithCredentials("gremlin-east-1", "tinkerpop-sigv4", provider)
		err := interceptor(req)

		assert.NoError(t, err)
		assert.NotEmpty(t, req.Headers.Get("X-Amz-Date"))
		authHeader := req.Headers.Get("Authorization")
		assert.True(t, strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 Credential=MOCK_ID"))
		assert.Contains(t, authHeader, "gremlin-east-1/tinkerpop-sigv4/aws4_request")
		assert.Contains(t, authHeader, "Signature=")
	})

	t.Run("adds session token when provided", func(t *testing.T) {
		req := newDeprecatedAuthRequest(t)
		assert.Empty(t, req.Headers.Get("X-Amz-Security-Token"))

		provider := &mockAuthCredentialsProvider{
			accessKey:    "MOCK_ID",
			secretKey:    "MOCK_KEY",
			sessionToken: "MOCK_TOKEN",
		}
		interceptor := SigV4AuthWithCredentials("gremlin-east-1", "tinkerpop-sigv4", provider)
		err := interceptor(req)

		assert.NoError(t, err)
		assert.Equal(t, "MOCK_TOKEN", req.Headers.Get("X-Amz-Security-Token"))
	})

	t.Run("SigV4Auth delegates to credential chain variant", func(t *testing.T) {
		// SigV4Auth with no explicit provider should still return a usable interceptor.
		interceptor := SigV4Auth("gremlin-east-1", "tinkerpop-sigv4")
		assert.NotNil(t, interceptor)
	})
}
