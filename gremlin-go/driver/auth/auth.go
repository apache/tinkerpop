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

// Package auth provides authentication interceptors for the gremlin-go driver.
// Each constructor returns a gremlingo.RequestInterceptor that can be assigned to
// the Auth field of ClientSettings or DriverRemoteConnectionSettings.
package auth

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"

	gremlingo "github.com/apache/tinkerpop/gremlin-go/v4/driver"
)

// Basic returns a RequestInterceptor that adds a Basic authentication header.
func Basic(username, password string) gremlingo.RequestInterceptor {
	encoded := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	return func(req *gremlingo.HttpRequest) error {
		req.Headers.Set(gremlingo.HeaderAuthorization, "Basic "+encoded)
		return nil
	}
}

// SigV4 returns a RequestInterceptor that signs requests using AWS SigV4.
// It uses the default AWS credential chain (env vars, shared config, IAM role, etc.)
func SigV4(region, service string) gremlingo.RequestInterceptor {
	return SigV4WithCredentials(region, service, nil)
}

// SigV4WithCredentials returns a RequestInterceptor that signs requests using AWS SigV4
// with the provided credentials provider. If provider is nil, uses default credential chain.
// If the request body has not been serialized yet (*RequestMessage), it is automatically
// serialized to JSON before signing via SerializeBody().
//
// Caches the signer and credentials provider for efficiency.
func SigV4WithCredentials(region, service string, credentialsProvider aws.CredentialsProvider) gremlingo.RequestInterceptor {
	// Create signer once - it's stateless and safe to reuse
	signer := v4.NewSigner()

	// Cache for resolved credentials provider (lazy initialization)
	var cachedProvider aws.CredentialsProvider
	var providerOnce sync.Once
	var providerErr error

	return func(req *gremlingo.HttpRequest) error {
		// Ensure body is serialized to JSON bytes before signing.
		// SerializeBody is idempotent: safe to call even if already serialized.
		if _, err := req.SerializeBody(); err != nil {
			return fmt.Errorf("SigV4 signing requires a serialized body: %w", err)
		}

		ctx := context.Background()

		// Resolve credentials provider once if not provided
		provider := credentialsProvider
		if provider == nil {
			providerOnce.Do(func() {
				cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
				if err != nil {
					providerErr = err
					return
				}
				cachedProvider = cfg.Credentials
			})
			if providerErr != nil {
				return providerErr
			}
			provider = cachedProvider
		}

		// Retrieve credentials (the provider handles caching internally)
		creds, err := provider.Retrieve(ctx)
		if err != nil {
			return err
		}

		// Signed header set: host, x-amz-date, x-amz-content-sha256, and (for session
		// credentials) x-amz-security-token. The request's own headers (accept, content-type,
		// ...) are deliberately NOT signed: the HTTP client manages transport headers such as
		// Content-Length and Accept-Encoding after this interceptor runs, so a signature covering
		// them would not match the bytes actually sent.

		// Strip the default port (443 for https, 80 for http) from the host so the signed
		// canonical Host and the Host header sent on the wire both omit it, matching what a
		// spec-compliant verifier reconstructs (a bare host). Mutating req.URL here is safe: the
		// driver parses a fresh URL per request, and this also fixes the Host sent on the wire.
		// Trim only the trailing ":port" so IPv6 literals keep their brackets (using
		// URL.Hostname() would drop them, yielding an unparseable host like "::1").
		port := req.URL.Port()
		if (req.URL.Scheme == "https" && port == "443") || (req.URL.Scheme == "http" && port == "80") {
			req.URL.Host = strings.TrimSuffix(req.URL.Host, ":"+port)
		}

		payloadHash := req.PayloadHash()
		stdReq, err := http.NewRequest(req.Method, req.URL.String(), nil)
		if err != nil {
			return err
		}
		stdReq.Host = req.URL.Host
		// Set the payload hash header BEFORE signing so it is part of the signed set:
		// aws-sdk-go-v2's SignHTTP takes the hash as a parameter but does not add the header
		// itself, and a present-but-unsigned header is rejected by the server.
		stdReq.Header.Set("X-Amz-Content-Sha256", payloadHash)

		if err := signer.SignHTTP(ctx, creds, stdReq, payloadHash, service, region, time.Now()); err != nil {
			return err
		}

		// Copy the SigV4 output headers back onto the request.
		for k, v := range stdReq.Header {
			req.Headers[k] = v
		}

		return nil
	}
}
