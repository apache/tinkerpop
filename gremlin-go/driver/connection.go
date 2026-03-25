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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

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
	wg           sync.WaitGroup
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
func (c *connection) submit(req *RequestMessage) (ResultSet, error) {
	rs := newChannelResultSet()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.executeAndStream(req, rs)
	}()

	return rs, nil
}

func (c *connection) executeAndStream(req *RequestMessage, rs ResultSet) {
	defer rs.Close()

	// Create HttpRequest for interceptors
	httpReq, err := NewHttpRequest(http.MethodPost, c.url)
	if err != nil {
		c.logHandler.logf(Error, failedToSendRequest, err.Error())
		rs.setError(err)
		return
	}

	// Set default headers before interceptors
	c.setHttpRequestHeaders(httpReq)

	// Set Body to the raw *RequestMessage so interceptors can inspect/modify it
	httpReq.Body = req

	// Apply interceptors — they see *RequestMessage in Body (pre-serialization).
	// Interceptors may replace Body with []byte, io.Reader, or *http.Request.
	for _, interceptor := range c.interceptors {
		if err := interceptor(httpReq); err != nil {
			c.logHandler.logf(Error, failedToSendRequest, err.Error())
			rs.setError(err)
			return
		}
	}

	// After interceptors, serialize if Body is still *RequestMessage
	if r, ok := httpReq.Body.(*RequestMessage); ok {
		if c.serializer != nil {
			data, err := c.serializer.SerializeMessage(r)
			if err != nil {
				c.logHandler.logf(Error, failedToSendRequest, err.Error())
				rs.setError(err)
				return
			}
			httpReq.Body = data
		} else {
			errMsg := "request body was not serialized; either provide a serializer or add an interceptor that serializes the request"
			c.logHandler.logf(Error, failedToSendRequest, errMsg)
			rs.setError(fmt.Errorf("%s", errMsg))
			return
		}
	}

	// Create actual http.Request from HttpRequest based on Body type
	var httpGoReq *http.Request
	switch body := httpReq.Body.(type) {
	case []byte:
		httpGoReq, err = http.NewRequest(httpReq.Method, httpReq.URL.String(), bytes.NewReader(body))
		if err != nil {
			c.logHandler.logf(Error, failedToSendRequest, err.Error())
			rs.setError(err)
			return
		}
		httpGoReq.Header = httpReq.Headers
	case io.Reader:
		httpGoReq, err = http.NewRequest(httpReq.Method, httpReq.URL.String(), body)
		if err != nil {
			c.logHandler.logf(Error, failedToSendRequest, err.Error())
			rs.setError(err)
			return
		}
		httpGoReq.Header = httpReq.Headers
	case *http.Request:
		httpGoReq = body
	default:
		errMsg := fmt.Sprintf("unsupported body type after interceptors: %T", body)
		c.logHandler.logf(Error, failedToSendRequest, errMsg)
		rs.setError(fmt.Errorf("%s", errMsg))
		return
	}

	resp, err := c.httpClient.Do(httpGoReq)
	if err != nil {
		c.logHandler.logf(Error, failedToSendRequest, err.Error())
		rs.setError(err)
		return
	}
	defer func() {
		// Drain any unread bytes so the connection can be reused gracefully.
		// Without this, Go's HTTP client sends a TCP RST instead of FIN,
		// causing "Connection reset by peer" errors on the server.
		io.Copy(io.Discard, resp.Body)
		if err := resp.Body.Close(); err != nil {
			c.logHandler.logf(Debug, failedToCloseResponseBody, err.Error())
		}
	}()

	// If the HTTP status indicates an error and the response is not GraphBinary,
	// read the body as a text/JSON error message instead of attempting binary
	// deserialization which would produce cryptic errors.
	contentType := resp.Header.Get(HeaderContentType)
	if resp.StatusCode >= 400 && !strings.Contains(contentType, graphBinaryMimeType) {
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			c.logHandler.logf(Error, failedToReceiveResponse, readErr.Error())
			rs.setError(fmt.Errorf("Gremlin Server returned HTTP %d and failed to read body: %w",
				resp.StatusCode, readErr))
			return
		}
		errorBody := string(bodyBytes)
		errorMsg := tryExtractJSONError(errorBody)
		if errorMsg == "" {
			errorMsg = fmt.Sprintf("Gremlin Server returned HTTP %d: %s", resp.StatusCode, errorBody)
		}
		c.logHandler.logf(Error, failedToReceiveResponse, errorMsg)
		rs.setError(fmt.Errorf("%s", errorMsg))
		return
	}

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
	d := NewGraphBinaryDeserializer(reader)
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
