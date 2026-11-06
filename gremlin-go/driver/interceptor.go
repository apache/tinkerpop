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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// Common HTTP header keys
const (
	HeaderContentType    = "Content-Type"
	HeaderAccept         = "Accept"
	HeaderUserAgent      = "User-Agent"
	HeaderAcceptEncoding = "Accept-Encoding"
	HeaderAuthorization  = "Authorization"
)

// HttpRequest represents an HTTP request that can be modified by interceptors.
type HttpRequest struct {
	Method  string
	URL     *url.URL
	Headers http.Header
	Body    any
}

// NewHttpRequest creates a new HttpRequest with the given method and URL.
func NewHttpRequest(method, rawURL string) (*HttpRequest, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	return &HttpRequest{
		Method:  method,
		URL:     u,
		Headers: make(http.Header),
	}, nil
}

// ToStdRequest converts HttpRequest to a standard http.Request for signing.
// Returns nil if the request cannot be created (invalid method or URL).
func (r *HttpRequest) ToStdRequest() (*http.Request, error) {
	var body io.Reader
	switch b := r.Body.(type) {
	case []byte:
		body = bytes.NewReader(b)
	default:
		body = http.NoBody
	}
	req, err := http.NewRequest(r.Method, r.URL.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header = r.Headers
	return req, nil
}

// PayloadHash returns the SHA256 hash of the request body for SigV4 signing.
func (r *HttpRequest) PayloadHash() string {
	switch b := r.Body.(type) {
	case []byte:
		if len(b) == 0 {
			return "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // SHA256 of empty string
		}
		h := sha256.Sum256(b)
		return hex.EncodeToString(h[:])
	default:
		return "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // SHA256 of empty string
	}
}

// SerializeBody serializes the request body to JSON if it is still a *RequestMessage.
// If the body is already []byte, it returns those bytes (idempotent).
// On successful serialization from *RequestMessage, it sets the body to the resulting bytes
// and updates the Content-Type and Content-Length headers.
func (r *HttpRequest) SerializeBody() ([]byte, error) {
	switch b := r.Body.(type) {
	case []byte:
		return b, nil
	case *RequestMessage:
		payload := make(map[string]interface{})
		payload["gremlin"] = b.Gremlin
		for k, v := range b.Fields {
			payload[k] = v
		}
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize request to JSON: %w", err)
		}
		r.Body = data
		r.Headers.Set(HeaderContentType, "application/json")
		r.Headers.Set("Content-Length", strconv.Itoa(len(data)))
		return data, nil
	default:
		return nil, fmt.Errorf("cannot serialize request body of type %T; expected *RequestMessage or []byte. "+
			"If an interceptor modified the body, it must set it to []byte or leave it as *RequestMessage", r.Body)
	}
}

// RequestInterceptor is a function that modifies an HTTP request before it is sent.
type RequestInterceptor func(*HttpRequest) error
