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
	"compress/zlib"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
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
	Body    []byte
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
	req, err := http.NewRequest(r.Method, r.URL.String(), bytes.NewReader(r.Body))
	if err != nil {
		return nil, err
	}
	req.Header = r.Headers
	return req, nil
}

// PayloadHash returns the SHA256 hash of the request body for SigV4 signing.
func (r *HttpRequest) PayloadHash() string {
	if len(r.Body) == 0 {
		return "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // SHA256 of empty string
	}
	h := sha256.Sum256(r.Body)
	return hex.EncodeToString(h[:])
}

// RequestInterceptor is a function that modifies an HTTP request before it is sent.
type RequestInterceptor func(*HttpRequest) error

// connectionSettings holds configuration for the connection.
type connectionSettings struct {
	tlsConfig                *tls.Config
	connectionTimeout        time.Duration
	maxConnsPerHost          int
	maxIdleConnsPerHost      int
	idleConnTimeout          time.Duration
	keepAliveInterval        time.Duration
	enableCompression        bool
	enableUserAgentOnConnect bool
}

// connection handles HTTP request/response for Gremlin queries.
// This is the transport layer for communicating with a Gremlin server.
type connection struct {
	url          string
	httpClient   *http.Client
	connSettings *connectionSettings
	logHandler   *logHandler
	serializer   *GraphBinarySerializer
	interceptors []RequestInterceptor
}

// Connection pool defaults aligned with Java driver
const (
	defaultMaxConnsPerHost     = 128               // Java: ConnectionPool.MAX_POOL_SIZE
	defaultMaxIdleConnsPerHost = 8                 // Keep some connections warm
	defaultIdleConnTimeout     = 180 * time.Second // Java: CONNECTION_IDLE_TIMEOUT_MILLIS
	defaultConnectionTimeout   = 15 * time.Second  // Java: CONNECTION_SETUP_TIMEOUT_MILLIS
	defaultKeepAliveInterval   = 30 * time.Second  // TCP keep-alive probe interval
)

func newConnection(handler *logHandler, url string, connSettings *connectionSettings) *connection {
	// Apply defaults for zero values
	connectionTimeout := connSettings.connectionTimeout
	if connectionTimeout == 0 {
		connectionTimeout = defaultConnectionTimeout
	}

	maxConnsPerHost := connSettings.maxConnsPerHost
	if maxConnsPerHost == 0 {
		maxConnsPerHost = defaultMaxConnsPerHost
	}

	maxIdleConnsPerHost := connSettings.maxIdleConnsPerHost
	if maxIdleConnsPerHost == 0 {
		maxIdleConnsPerHost = defaultMaxIdleConnsPerHost
	}

	idleConnTimeout := connSettings.idleConnTimeout
	if idleConnTimeout == 0 {
		idleConnTimeout = defaultIdleConnTimeout
	}

	keepAliveInterval := connSettings.keepAliveInterval
	if keepAliveInterval == 0 {
		keepAliveInterval = defaultKeepAliveInterval
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   connectionTimeout,
			KeepAlive: keepAliveInterval,
		}).DialContext,
		TLSClientConfig:     connSettings.tlsConfig,
		MaxConnsPerHost:     maxConnsPerHost,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		IdleConnTimeout:     idleConnTimeout,
		DisableCompression:  !connSettings.enableCompression,
	}

	return &connection{
		url:          url,
		httpClient:   &http.Client{Transport: transport}, // No Timeout - allows streaming
		connSettings: connSettings,
		logHandler:   handler,
		serializer:   newGraphBinarySerializer(handler),
	}
}

// AddInterceptor adds a request interceptor to the chain.
func (c *connection) AddInterceptor(interceptor RequestInterceptor) {
	c.interceptors = append(c.interceptors, interceptor)
}

// submit sends request and streams results directly to ResultSet
func (c *connection) submit(req *request) (ResultSet, error) {
	rs := newChannelResultSet()

	data, err := c.serializer.SerializeMessage(req)
	if err != nil {
		rs.Close()
		return rs, err
	}

	go c.executeAndStream(data, rs)

	return rs, nil
}

func (c *connection) executeAndStream(data []byte, rs ResultSet) {
	defer rs.Close()

	// Create HttpRequest for interceptors
	httpReq, err := NewHttpRequest(http.MethodPost, c.url)
	if err != nil {
		c.logHandler.logf(Error, failedToSendRequest, err.Error())
		rs.setError(err)
		return
	}
	httpReq.Body = data

	// Set default headers before interceptors
	c.setHttpRequestHeaders(httpReq)

	// Apply interceptors
	for _, interceptor := range c.interceptors {
		if err := interceptor(httpReq); err != nil {
			c.logHandler.logf(Error, failedToSendRequest, err.Error())
			rs.setError(err)
			return
		}
	}

	// Create actual http.Request from HttpRequest
	req, err := http.NewRequest(httpReq.Method, httpReq.URL.String(), bytes.NewReader(httpReq.Body))
	if err != nil {
		c.logHandler.logf(Error, failedToSendRequest, err.Error())
		rs.setError(err)
		return
	}
	req.Header = httpReq.Headers

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logHandler.logf(Error, failedToSendRequest, err.Error())
		rs.setError(err)
		return
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			c.logHandler.logf(Debug, failedToCloseResponseBody, err.Error())
		}
	}()

	reader, zlibReader, err := c.getReader(resp)
	if err != nil {
		c.logHandler.logf(Error, failedToReceiveResponse, err.Error())
		rs.setError(err)
		return
	}
	if zlibReader != nil {
		defer func() {
			if err := zlibReader.Close(); err != nil {
				c.logHandler.logf(Debug, failedToCloseDecompReader, err.Error())
			}
		}()
	}

	c.streamToResultSet(reader, rs)
}

// setHttpRequestHeaders sets default headers on HttpRequest (for interceptors)
func (c *connection) setHttpRequestHeaders(req *HttpRequest) {
	req.Headers.Set(HeaderContentType, graphBinaryMimeType)
	req.Headers.Set(HeaderAccept, graphBinaryMimeType)

	if c.connSettings.enableUserAgentOnConnect {
		req.Headers.Set(HeaderUserAgent, userAgent)
	}
	if c.connSettings.enableCompression {
		req.Headers.Set(HeaderAcceptEncoding, "deflate")
	}
}

func (c *connection) getReader(resp *http.Response) (io.Reader, io.Closer, error) {
	if resp.Header.Get("Content-Encoding") == "deflate" {
		zr, err := zlib.NewReader(resp.Body)
		if err != nil {
			return nil, nil, err
		}
		return zr, zr, nil
	}
	return resp.Body, nil, nil
}

func (c *connection) streamToResultSet(reader io.Reader, rs ResultSet) {
	d := NewStreamingDeserializer(reader)
	if err := d.ReadHeader(); err != nil {
		if err != io.EOF {
			c.logHandler.logf(Error, failedToReceiveResponse, err.Error())
			rs.setError(err)
		}
		return
	}

	for {
		obj, err := d.ReadFullyQualified()
		if err != nil {
			if err != io.EOF {
				c.logHandler.logf(Error, failedToReceiveResponse, err.Error())
				rs.setError(err)
			}
			return
		}

		if marker, ok := obj.(Marker); ok && marker == EndOfStream() {
			code, msg, _, err := d.ReadStatus()
			if err != nil {
				c.logHandler.logf(Error, failedToReceiveResponse, err.Error())
				rs.setError(err)
				return
			}
			if code != 200 && code != 0 {
				rs.setError(newError(err0502ResponseReadLoopError, msg, code))
			}
			return
		}

		rs.Channel() <- &Result{obj}
	}
}

func (c *connection) close() {
	c.httpClient.CloseIdleConnections()
}
