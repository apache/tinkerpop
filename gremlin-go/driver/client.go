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
	"reflect"
	"sync"
	"time"

	"golang.org/x/text/language"
)

// ClientSettings is used to modify a Client's settings on initialization.
type ClientSettings struct {
	TraversalSource string
	LogVerbosity    LogVerbosity
	Logger          Logger
	Language        language.Tag

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

	// Proxy returns the proxy URL to use for a given request. When nil, the client
	// uses http.ProxyFromEnvironment (HTTP_PROXY/HTTPS_PROXY/NO_PROXY).
	Proxy func(*http.Request) (*url.URL, error)

	EnableUserAgentOnConnect bool

	// PDTRegistry enables automatic hydration of ProviderDefinedType values during deserialization.
	PDTRegistry *PDTRegistry

	// Interceptors are functions that modify HTTP requests before sending.
	Interceptors []RequestInterceptor

	// Auth is a RequestInterceptor for authentication (e.g. auth.Basic, auth.SigV4).
	// As a convenience, this is always appended to the end of the interceptor list
	// so it runs last, after any user interceptors have modified the request.
	Auth RequestInterceptor
}

// Client is used to connect and interact with a Gremlin-supported server.
type Client struct {
	url                string
	traversalSource    string
	logHandler         *logHandler
	connectionSettings *connectionSettings
	conn               *connection
	bulkResults        bool     // connection-level default for bulkResults (default false)
	transactions       sync.Map // tracks open transactions for cascade rollback on close
}

// NewClient creates a Client and configures it with the given parameters.
// Important note: to avoid leaking a connection, always close the Client.
func NewClient(url string, configurations ...func(settings *ClientSettings)) (*Client, error) {
	settings := &ClientSettings{
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

	return client, nil
}

// Close closes the client via connection.
// This is idempotent due to the underlying close() methods being idempotent as well.
func (client *Client) Close() {
	client.logHandler.logf(Info, closeClient, client.url)
	// Best-effort rollback of any open transactions before closing connections
	client.transactions.Range(func(key, value interface{}) bool {
		if tx, ok := value.(*Transaction); ok {
			tx.Close()
		}
		return true
	})
	if client.conn != nil {
		client.conn.close()
	}
}

func (client *Client) errorCallback() {
	client.logHandler.log(Error, errorCallback)
}

// Transact creates a new Transaction for executing operations within an explicit
// server-side transaction. Transactions are short-lived and single-use: after
// commit or rollback, create a new Transaction for the next unit of work.
// The traversal source (g alias) is inherited from this Client.
func (client *Client) Transact() *Transaction {
	return &Transaction{client: client}
}

func (client *Client) trackTransaction(tx *Transaction) {
	client.transactions.Store(tx, tx)
}

func (client *Client) untrackTransaction(tx *Transaction) {
	client.transactions.Delete(tx)
}

// SubmitWithOptions submits a Gremlin script to the server with specified RequestOptions and returns a ResultSet.
func (client *Client) SubmitWithOptions(traversalString string, requestOptions RequestOptions) (ResultSet, error) {
	client.logHandler.logf(Debug, submitStartedString, traversalString)
	// Apply the connection-level bulkResults default when the request did not set it
	// per-request. The script path only forces bulking when the connection-level
	// setting is true; a false setting leaves the request untouched (matching the
	// other GLVs, whose connection-level bulkResults defaults to false).
	if requestOptions.bulkResults == nil && client.bulkResults {
		bulk := true
		requestOptions.bulkResults = &bulk
	}
	request := MakeStringRequest(traversalString, client.traversalSource, requestOptions)
	rs, err := client.conn.submit(&request)
	return rs, err
}

// Submit submits a Gremlin script to the server and returns a ResultSet. Submit can optionally accept a map of parameters
// to be applied to the traversalString, it is preferred however to instead wrap any parameters into a RequestOptions
// struct and use SubmitWithOptions().
func (client *Client) Submit(traversalString string, parameters ...map[string]interface{}) (ResultSet, error) {
	requestOptionsBuilder := new(RequestOptionsBuilder)
	if len(parameters) > 0 {
		requestOptionsBuilder.SetParameters(parameters[0])
	}
	return client.SubmitWithOptions(traversalString, requestOptionsBuilder.Create())
}

// submitGremlinLang submits GremlinLang to the server to execute and returns a ResultSet.
func (client *Client) submitGremlinLang(gremlinLang *GremlinLang) (ResultSet, error) {
	return client.submitGremlinLangWithBuilder(gremlinLang, new(RequestOptionsBuilder))
}

// submitGremlinLangWithBuilder is the core submission function. It extracts parameters
// and options strategies from the GremlinLang, merges them into the provided builder,
// then builds and sends the request.
func (client *Client) submitGremlinLangWithBuilder(gremlinLang *GremlinLang, builder *RequestOptionsBuilder) (ResultSet, error) {
	client.logHandler.logf(Debug, submitStartedString, *gremlinLang)

	parametersString := gremlinLang.GetParametersAsString()
	if parametersString != "[:]" {
		builder.SetParametersString(parametersString)
	}

	if len(gremlinLang.optionsStrategies) > 0 {
		builder = applyOptionsConfig(builder, gremlinLang.optionsStrategies[0].configuration)
	}

	// default bulkResults to true when using DRC through request options
	// consistent with Java RequestOptions.getRequestOptions and Python extract_request_options.
	// The traversal path always defaults to true when unset, regardless of the
	// connection-level BulkResults setting (matching the other GLVs).
	if builder.bulkResults == nil {
		builder.SetBulkResults(true)
	}

	request := MakeStringRequest(gremlinLang.GetGremlin(), client.traversalSource, builder.Create())
	return client.conn.submit(&request)
}

func applyOptionsConfig(builder *RequestOptionsBuilder, config map[string]interface{}) *RequestOptionsBuilder {
	builderValue := reflect.ValueOf(builder)

	// Map configuration keys to setter method names.
	// "parameters" is intentionally excluded because parameters are handled separately
	// via SetParametersString in submitGremlinLang, and including it here would
	// trigger the mutual exclusion panic between SetParameters and SetParametersString.
	setterMap := map[string]string{
		"evaluationTimeout":     "SetEvaluationTimeout",
		"batchSize":             "SetBatchSize",
		"userAgent":             "SetUserAgent",
		"materializeProperties": "SetMaterializeProperties",
		"bulkResults":           "SetBulkResults",
	}

	for key, value := range config {
		if methodName, exists := setterMap[key]; exists {
			method := builderValue.MethodByName(methodName)
			if method.IsValid() {
				args := []reflect.Value{reflect.ValueOf(value)}
				method.Call(args)
			}
		}
	}

	return builder
}
