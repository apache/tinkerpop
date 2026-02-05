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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
)

// BasicAuth returns a RequestInterceptor that adds Basic authentication header.
func BasicAuth(username, password string) RequestInterceptor {
	encoded := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	return func(req *HttpRequest) error {
		req.Headers.Set(HeaderAuthorization, "Basic "+encoded)
		return nil
	}
}

// Sigv4Auth returns a RequestInterceptor that signs requests using AWS SigV4.
// It uses the default AWS credential chain (env vars, shared config, IAM role, etc.)
func Sigv4Auth(region, service string) RequestInterceptor {
	return Sigv4AuthWithCredentials(region, service, nil)
}

// Sigv4AuthWithCredentials returns a RequestInterceptor that signs requests using AWS SigV4
// with the provided credentials provider. If provider is nil, uses default credential chain.
//
// Caches the signer and credentials provider for efficiency.
func Sigv4AuthWithCredentials(region, service string, credentialsProvider aws.CredentialsProvider) RequestInterceptor {
	// Create signer once - it's stateless and safe to reuse
	signer := v4.NewSigner()

	// Cache for resolved credentials provider (lazy initialization)
	var cachedProvider aws.CredentialsProvider
	var providerOnce sync.Once
	var providerErr error

	return func(req *HttpRequest) error {
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

		stdReq, err := req.ToStdRequest()
		if err != nil {
			return err
		}
		stdReq.Body = nil // Body is handled separately via payload hash

		if err := signer.SignHTTP(ctx, creds, stdReq, req.PayloadHash(), service, region, time.Now()); err != nil {
			return err
		}

		// Copy signed headers back to HttpRequest
		for k, v := range stdReq.Header {
			req.Headers[k] = v
		}

		return nil
	}
}
