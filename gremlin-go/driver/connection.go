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
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Compression identifies the content-encoding negotiated with the Gremlin server.
type Compression string

const (
	// CompressionNone disables compression. No Accept-Encoding header is sent.
	CompressionNone Compression = "none"
	// CompressionDeflate requests per-chunk deflate compression from the server (default).
	CompressionDeflate Compression = "deflate"
)

// connectionSettings holds configuration for the connection.
type connectionSettings struct {
	ssl                      *tls.Config
	connectTimeout           time.Duration
	readTimeout              time.Duration
	maxConnsPerHost          int
	maxIdleConnsPerHost      int
	idleTimeout              time.Duration
	keepAliveTime            time.Duration
	compression              Compression
	maxResponseHeaderBytes   int64
	batchSize                int
	proxy                    func(*http.Request) (*url.URL, error)
	enableUserAgentOnConnect bool
	pdtRegistry              *PDTRegistry
}

// connection handles HTTP request/response for Gremlin queries.
// This is the transport layer for communicating with a Gremlin server.
type connection struct {
	url          string
	httpClient   *http.Client
	connSettings *connectionSettings
	logHandler   *logHandler
	interceptors []RequestInterceptor
	wg           sync.WaitGroup
}

// Connection pool defaults aligned with Java driver
const (
	defaultMaxConnsPerHost     = 128               // Java: ConnectionPool.MAX_POOL_SIZE
	defaultMaxIdleConnsPerHost = 8                 // Keep some connections warm
	defaultIdleTimeout         = 180 * time.Second // Java: CONNECTION_IDLE_TIMEOUT_MILLIS
	defaultConnectTimeout      = 5 * time.Second   // TCP/transport-establishment timeout
	defaultKeepAliveTime       = 30 * time.Second  // TCP keep-alive idle-before-probe interval
	defaultBatchSizeValue      = 64                // Java: resultIterationBatchSize default
)

// resolveTimeout reconciles a duration option with its millisecond companion. The
// *Millis form is the canonical/documented option; the time.Duration form is the
// idiomatic Go companion. Supplying both (each non-zero) is a configuration error.
// A zero result means "unset", letting the caller apply its default.
func resolveTimeout(millis int, duration time.Duration, name string) (time.Duration, error) {
	if millis != 0 && duration != 0 {
		return 0, fmt.Errorf("set only one of %sMillis or %s, not both", name, name)
	}
	if millis != 0 {
		return time.Duration(millis) * time.Millisecond, nil
	}
	return duration, nil
}

func newConnection(handler *logHandler, url string, connSettings *connectionSettings) *connection {
	// Apply defaults for zero values
	connectTimeout := connSettings.connectTimeout
	if connectTimeout == 0 {
		connectTimeout = defaultConnectTimeout
	}

	maxConnsPerHost := connSettings.maxConnsPerHost
	if maxConnsPerHost == 0 {
		maxConnsPerHost = defaultMaxConnsPerHost
	}

	maxIdleConnsPerHost := connSettings.maxIdleConnsPerHost
	if maxIdleConnsPerHost == 0 {
		maxIdleConnsPerHost = defaultMaxIdleConnsPerHost
	}

	idleTimeout := connSettings.idleTimeout
	if idleTimeout == 0 {
		idleTimeout = defaultIdleTimeout
	}

	keepAliveTime := connSettings.keepAliveTime
	if keepAliveTime == 0 {
		keepAliveTime = defaultKeepAliveTime
	}

	// Default the proxy resolver to the environment (HTTP_PROXY/HTTPS_PROXY/NO_PROXY)
	// unless an explicit override was provided. A custom http.Transport otherwise
	// silently drops environment proxy configuration.
	proxy := connSettings.proxy
	if proxy == nil {
		proxy = http.ProxyFromEnvironment
	}

	dialer := &net.Dialer{
		Timeout:   connectTimeout,
		KeepAlive: keepAliveTime,
	}

	readTimeout := connSettings.readTimeout
	dialContext := dialer.DialContext
	if readTimeout > 0 {
		// Wrap each dialed connection so every Read re-arms the read deadline.
		// This models an idle-read (per-read) timeout rather than a whole-request
		// deadline, and resets correctly across pooled-connection reuse because the
		// deadline is refreshed on every Read regardless of which request reuses it.
		dialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &readTimeoutConn{Conn: conn, timeout: readTimeout}, nil
		}
	}

	transport := &http.Transport{
		Proxy:                  proxy,
		DialContext:            dialContext,
		TLSClientConfig:        connSettings.ssl,
		MaxConnsPerHost:        maxConnsPerHost,
		MaxIdleConnsPerHost:    maxIdleConnsPerHost,
		IdleConnTimeout:        idleTimeout,
		MaxResponseHeaderBytes: connSettings.maxResponseHeaderBytes,
		// The server compresses per-GraphBinary-chunk (deflate) rather than using
		// generic HTTP compression, so the manual decode path in getReader handles
		// decompression. Disable net/http's transparent (gzip-only) handling.
		DisableCompression: true,
	}

	return &connection{
		url:          url,
		httpClient:   &http.Client{Transport: transport}, // No Timeout - allows streaming
		connSettings: connSettings,
		logHandler:   handler,
	}
}

// AddInterceptor adds a request interceptor to the chain.
func (c *connection) AddInterceptor(interceptor RequestInterceptor) {
	c.interceptors = append(c.interceptors, interceptor)
}

// applyDefaultBatchSize fills the request's batchSize field with the connection-level
// default when the per-request value is unset. This is a client-side default-fill; it
// adds no wire field unless a batch size is in effect.
func (c *connection) applyDefaultBatchSize(req *RequestMessage) {
	if req == nil || c.connSettings == nil {
		return
	}
	batchSize := c.connSettings.batchSize
	if batchSize == 0 {
		batchSize = defaultBatchSizeValue
	}
	if req.Fields == nil {
		req.Fields = make(map[string]interface{})
	}
	if _, ok := req.Fields["batchSize"]; !ok {
		req.Fields["batchSize"] = batchSize
	}
}

// submit sends request and streams results directly to ResultSet.
// Blocks until response headers arrive, ensuring the server has acknowledged
// receipt of the request before returning.
func (c *connection) submit(req *RequestMessage) (ResultSet, error) {
	rs := newChannelResultSet()

	// Fill the connection-level default batchSize when the request did not set one.
	c.applyDefaultBatchSize(req)

	// Send the HTTP request synchronously — blocks until response headers arrive
	resp, err := c.sendRequest(req)
	if err != nil {
		rs.Close()
		return nil, err
	}

	// If the HTTP status indicates an error and the response is not GraphBinary,
	// read the body as a text/JSON error message instead of attempting binary
	// deserialization which would produce cryptic errors.
	contentType := resp.Header.Get(HeaderContentType)
	if resp.StatusCode >= 400 && !strings.Contains(contentType, graphBinaryMimeType) {
		defer func() {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			rs.Close()
			return nil, fmt.Errorf("Gremlin Server returned HTTP %d and failed to read body: %w",
				resp.StatusCode, readErr)
		}
		errorBody := string(bodyBytes)
		errorMsg := tryExtractJSONError(errorBody)
		if errorMsg == "" {
			errorMsg = fmt.Sprintf("Gremlin Server returned HTTP %d: %s", resp.StatusCode, errorBody)
		}
		c.logHandler.logf(Error, failedToReceiveResponse, errorMsg)
		rs.Close()
		return nil, fmt.Errorf("%s", errorMsg)
	}

	// Stream the response body into the ResultSet asynchronously
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		// Drain any unread bytes so the connection can be reused gracefully.
		// Without this, Go's HTTP client sends a TCP RST instead of FIN,
		// causing "Connection reset by peer" errors on the server.
		defer func() {
			io.Copy(io.Discard, resp.Body)
			if err := resp.Body.Close(); err != nil {
				c.logHandler.logf(Debug, failedToCloseResponseBody, err.Error())
			}
		}()
		defer rs.Close()
		c.streamResponse(resp, rs)
	}()

	return rs, nil
}

// sendRequest builds and sends the HTTP request, blocking until response headers arrive.
func (c *connection) sendRequest(req *RequestMessage) (*http.Response, error) {
	// Create HttpRequest for interceptors
	httpReq, err := NewHttpRequest(http.MethodPost, c.url)
	if err != nil {
		c.logHandler.logf(Error, failedToSendRequest, err.Error())
		return nil, err
	}

	// Set default headers before interceptors
	c.setHttpRequestHeaders(httpReq)

	// Set Body to the raw *RequestMessage so interceptors can inspect/modify it
	httpReq.Body = req

	// Promote transactionId to HTTP header before interceptors and serialization
	c.promoteTransactionIdHeader(httpReq, req)

	// Apply interceptors — they see *RequestMessage in Body (pre-serialization).
	// Interceptors may replace Body with []byte, io.Reader, or *http.Request.
	for _, interceptor := range c.interceptors {
		if err := interceptor(httpReq); err != nil {
			c.logHandler.logf(Error, failedToSendRequest, err.Error())
			return nil, err
		}
	}

	// After interceptors, auto-serialize the body to JSON if still a *RequestMessage.
	// SerializeBody is idempotent: if an interceptor already called it, this is a no-op.
	if _, ok := httpReq.Body.(*RequestMessage); ok {
		if _, err := httpReq.SerializeBody(); err != nil {
			c.logHandler.logf(Error, failedToSendRequest, err.Error())
			return nil, err
		}
	}

	// Create actual http.Request from HttpRequest based on Body type
	var httpGoReq *http.Request
	switch body := httpReq.Body.(type) {
	case []byte:
		httpGoReq, err = http.NewRequest(httpReq.Method, httpReq.URL.String(), bytes.NewReader(body))
		if err != nil {
			c.logHandler.logf(Error, failedToSendRequest, err.Error())
			return nil, err
		}
		httpGoReq.Header = httpReq.Headers
	case io.Reader:
		httpGoReq, err = http.NewRequest(httpReq.Method, httpReq.URL.String(), body)
		if err != nil {
			c.logHandler.logf(Error, failedToSendRequest, err.Error())
			return nil, err
		}
		httpGoReq.Header = httpReq.Headers
	case *http.Request:
		httpGoReq = body
	default:
		errMsg := fmt.Sprintf("unsupported body type after interceptors: %T", body)
		c.logHandler.logf(Error, failedToSendRequest, errMsg)
		return nil, fmt.Errorf("%s", errMsg)
	}

	// This blocks until response headers arrive
	resp, err := c.httpClient.Do(httpGoReq)
	if err != nil {
		c.logHandler.logf(Error, failedToSendRequest, err.Error())
		return nil, err
	}

	return resp, nil
}

// streamResponse reads the response body and pushes results into the ResultSet.
func (c *connection) streamResponse(resp *http.Response, rs ResultSet) {
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
	req.Headers.Set(HeaderAccept, graphBinaryMimeType)

	if c.connSettings.enableUserAgentOnConnect {
		req.Headers.Set(HeaderUserAgent, userAgent)
	}
	if c.connSettings.compression == CompressionDeflate {
		req.Headers.Set(HeaderAcceptEncoding, "deflate")
	}
}

// promoteTransactionIdHeader extracts transactionId from the request message
// fields and sets it as the X-Transaction-Id HTTP header. The field remains in
// the body for dual transmission per the HTTP transaction protocol spec.
func (c *connection) promoteTransactionIdHeader(req *HttpRequest, msg *RequestMessage) {
	if msg == nil {
		return
	}
	if txId, ok := msg.Fields["transactionId"]; ok {
		if txIdStr, ok := txId.(string); ok && txIdStr != "" {
			req.Headers.Set("X-Transaction-Id", txIdStr)
		}
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
	var d *GraphBinaryDeserializer
	if c.connSettings.pdtRegistry != nil {
		d = NewGraphBinaryDeserializerWithRegistry(reader, c.connSettings.pdtRegistry)
	} else {
		d = NewGraphBinaryDeserializer(reader)
	}
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

		if d.IsBulked() {
			bulkObj, err := d.ReadFullyQualified()
			if err != nil {
				c.logHandler.logf(Error, failedToReceiveResponse, err.Error())
				rs.setError(err)
				return
			}
			bulk, ok := bulkObj.(int64)
			if !ok {
				c.logHandler.logf(Error, failedToReceiveResponse, "expected int64 bulk count")
				rs.setError(fmt.Errorf("expected int64 bulk count, got %T", bulkObj))
				return
			}
			rs.Channel() <- &Result{&Traverser{Bulk: bulk, Value: obj}}
		} else {
			rs.Channel() <- &Result{obj}
		}
	}
}

// tryExtractJSONError attempts to extract an error message from a JSON response body.
// The server sometimes responds with a JSON object containing a "message" field
// even when it cannot produce a GraphBinary response.
func tryExtractJSONError(body string) string {
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(body), &obj); err != nil {
		return ""
	}
	if msg, ok := obj["message"]; ok {
		if s, ok := msg.(string); ok {
			return s
		}
	}
	return ""
}

func (c *connection) close() {
	c.wg.Wait()
	c.httpClient.CloseIdleConnections()
}

// readTimeoutConn wraps a net.Conn to enforce a per-read (idle-read) timeout.
// Each Read resets the read deadline to now+timeout, so the deadline measures the
// gap between reads rather than the total request duration. Because the deadline is
// re-armed on every Read, it resets correctly when a pooled connection is reused for
// a subsequent request.
type readTimeoutConn struct {
	net.Conn
	timeout time.Duration
}

func (c *readTimeoutConn) Read(b []byte) (int, error) {
	if err := c.Conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Read(b)
}
