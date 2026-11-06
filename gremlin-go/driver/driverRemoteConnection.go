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
	"crypto/tls"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/text/language"
)

// DriverRemoteConnectionSettings are used to configure the DriverRemoteConnection.
type DriverRemoteConnectionSettings struct {
	TraversalSource          string
	LogVerbosity             LogVerbosity
	Logger                   Logger
	Language                 language.Tag
	EnableUserAgentOnConnect bool

	// Ssl is the TLS configuration used for secure (wss/https) connections.
	Ssl *tls.Config

	// ConnectTimeoutMillis is the TCP/transport-establishment timeout in milliseconds
	// (TCP connect plus TLS handshake where applicable), not an HTTP request timeout.
	// This is the canonical form; ConnectTimeout is the time.Duration companion. Set
	// only one of the two.
	// Default: 5000 (5 seconds). Set to 0 to use the default.
	ConnectTimeoutMillis int

	// ConnectTimeout is the time.Duration companion to ConnectTimeoutMillis.
	ConnectTimeout time.Duration

	// ReadTimeoutMillis is an idle-read timeout in milliseconds: it is reset on each
	// read of the response body rather than bounding the whole request. Streaming-safe.
	// The deadline is re-armed across pooled-connection reuse. This is the canonical
	// form; ReadTimeout is the time.Duration companion. Set only one of the two.
	// Default: 0 (disabled).
	ReadTimeoutMillis int

	// ReadTimeout is the time.Duration companion to ReadTimeoutMillis.
	ReadTimeout time.Duration

	// Compression selects the content-encoding negotiated with the server.
	// Default: CompressionDeflate (on). Set to CompressionNone to disable compression.
	Compression Compression

	// MaxConnections is the maximum number of concurrent TCP connections
	// to the Gremlin server. This limits how many requests can be in-flight simultaneously.
	// Default: 128. Set to 0 to use the default.
	MaxConnections int

	// MaxIdleConnections is the maximum number of idle (keep-alive) connections to retain
	// in the connection pool. Idle connections are reused for subsequent requests.
	// Default: 8. Set to 0 to use the default.
	MaxIdleConnections int

	// IdleTimeoutMillis is how long in milliseconds an idle connection remains in the
	// pool before being closed. Set this to match your server's idle timeout if needed.
	// This is the canonical form; IdleTimeout is the time.Duration companion. Set only
	// one of the two.
	// Default: 180000 (180 seconds). Set to 0 to use the default.
	IdleTimeoutMillis int

	// IdleTimeout is the time.Duration companion to IdleTimeoutMillis.
	IdleTimeout time.Duration

	// KeepAliveTimeMillis is the TCP keep-alive idle-before-probe interval in
	// milliseconds on connections. This helps detect dead connections and keeps
	// connections alive through firewalls. This is the canonical form; KeepAliveTime is
	// the time.Duration companion. Set only one of the two.
	// Default: 30000 (30 seconds). Set to 0 to use the default.
	KeepAliveTimeMillis int

	// KeepAliveTime is the time.Duration companion to KeepAliveTimeMillis.
	KeepAliveTime time.Duration

	// BatchSize is the connection-level default that fills a request's batchSize
	// when it is not set per-request.
	// Default: 64. Set to 0 to use the default.
	BatchSize int

	// BulkResults is the connection-level default for bulkResults. When true, requests
	// submitted on this connection bulk results unless overridden per-request via
	// RequestOptionsBuilder.SetBulkResults. The DriverRemoteConnection traversal path
	// defaults to true regardless of this setting.
	// Default: false.
	BulkResults bool

	// MaxResponseHeaderBytes limits the number of response header bytes the client will
	// read. Maps to http.Transport.MaxResponseHeaderBytes.
	// Default: 0 (use net/http's default).
	MaxResponseHeaderBytes int64

	// Proxy returns the proxy URL to use for a given request. When nil, the connection
	// uses http.ProxyFromEnvironment (HTTP_PROXY/HTTPS_PROXY/NO_PROXY).
	Proxy func(*http.Request) (*url.URL, error)

	// Interceptors are functions that modify HTTP requests before sending.
	Interceptors []RequestInterceptor

	// Auth is a RequestInterceptor for authentication (e.g. auth.Basic, auth.SigV4).
	// As a convenience, this is always appended to the end of the interceptor list
	// so it runs last, after any user interceptors have modified the request.
	Auth RequestInterceptor

	// PDTRegistry enables registry-based dehydration in the gremlin-lang translator.
	PDTRegistry *PDTRegistry
}

// DriverRemoteConnection is a remote connection.
type DriverRemoteConnection struct {
	client   *Client
	isClosed bool
	settings *DriverRemoteConnectionSettings
}

// NewDriverRemoteConnection creates a new DriverRemoteConnection.
// If no custom connection settings are passed in, a connection will be created with "g" as the default TraversalSource,
// Info as the default LogVerbosity, a default logger struct, and English and as the
// default language
func NewDriverRemoteConnection(
	url string,
	configurations ...func(settings *DriverRemoteConnectionSettings)) (*DriverRemoteConnection, error) {
	settings := &DriverRemoteConnectionSettings{
		TraversalSource:          "g",
		LogVerbosity:             Info,
		Logger:                   &defaultLogger{},
		Language:                 language.English,
		Ssl:                      &tls.Config{},
		Compression:              CompressionDeflate,
		EnableUserAgentOnConnect: true,

		MaxConnections:     0, // Use default (128)
		MaxIdleConnections: 0, // Use default (8)
		IdleTimeout:        0, // Use default (180s)
		KeepAliveTime:      0, // Use default (30s)
		BatchSize:          0, // Use default (64)
	}
	for _, configuration := range configurations {
		configuration(settings)
	}

	connectTimeout, err := resolveTimeout(settings.ConnectTimeoutMillis, settings.ConnectTimeout, "ConnectTimeout")
	if err != nil {
		return nil, err
	}
	readTimeout, err := resolveTimeout(settings.ReadTimeoutMillis, settings.ReadTimeout, "ReadTimeout")
	if err != nil {
		return nil, err
	}
	idleTimeout, err := resolveTimeout(settings.IdleTimeoutMillis, settings.IdleTimeout, "IdleTimeout")
	if err != nil {
		return nil, err
	}
	keepAliveTime, err := resolveTimeout(settings.KeepAliveTimeMillis, settings.KeepAliveTime, "KeepAliveTime")
	if err != nil {
		return nil, err
	}

	connSettings := &connectionSettings{
		ssl:                      settings.Ssl,
		connectTimeout:           connectTimeout,
		readTimeout:              readTimeout,
		maxConnsPerHost:          settings.MaxConnections,
		maxIdleConnsPerHost:      settings.MaxIdleConnections,
		idleTimeout:              idleTimeout,
		keepAliveTime:            keepAliveTime,
		compression:              settings.Compression,
		maxResponseHeaderBytes:   settings.MaxResponseHeaderBytes,
		batchSize:                settings.BatchSize,
		proxy:                    settings.Proxy,
		enableUserAgentOnConnect: settings.EnableUserAgentOnConnect,
		pdtRegistry:              settings.PDTRegistry,
	}

	logHandler := newLogHandler(settings.Logger, settings.LogVerbosity, settings.Language)

	conn := newConnection(logHandler, url, connSettings)

	// Add user-provided interceptors
	for _, interceptor := range settings.Interceptors {
		conn.AddInterceptor(interceptor)
	}

	// Auth interceptor is always last so it runs after user interceptors
	if settings.Auth != nil {
		conn.AddInterceptor(settings.Auth)
	}

	client := &Client{
		url:                url,
		traversalSource:    settings.TraversalSource,
		logHandler:         logHandler,
		connectionSettings: connSettings,
		conn:               conn,
		bulkResults:        settings.BulkResults,
	}

	return &DriverRemoteConnection{client: client, isClosed: false, settings: settings}, nil
}

// Close closes the DriverRemoteConnection.
// Errors if any will be logged
func (driver *DriverRemoteConnection) Close() {
	driver.client.logHandler.logf(Info, closeDriverRemoteConnection, driver.client.url)
	driver.client.Close()
	driver.isClosed = true
}

// SubmitWithOptions sends a string traversal to the server along with specified RequestOptions.
func (driver *DriverRemoteConnection) SubmitWithOptions(traversalString string, requestOptions RequestOptions) (ResultSet, error) {
	result, err := driver.client.SubmitWithOptions(traversalString, requestOptions)
	if err != nil {
		driver.client.logHandler.logf(Error, logErrorGeneric, "Driver.Submit()", err.Error())
	}
	return result, err
}

// Submit sends a string traversal to the server.
func (driver *DriverRemoteConnection) Submit(traversalString string) (ResultSet, error) {
	return driver.SubmitWithOptions(traversalString, *new(RequestOptions))
}

// submitGremlinLang sends a GremlinLang traversal to the server.
func (driver *DriverRemoteConnection) submitGremlinLang(gremlinLang *GremlinLang) (ResultSet, error) {
	if driver.isClosed {
		return nil, newError(err0203SubmitGremlinLangToClosedConnectionError)
	}
	return driver.client.submitGremlinLang(gremlinLang)
}
