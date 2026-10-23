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
	"math"
	"net/http"
	"net/url"
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

	// PDTRegistry enables automatic hydration of CompositePDT values during deserialization.
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
	// Numeric options accept any numeric type holding an exact integer (see toInt), so an option supplied
	// as int32, int64 or float64 is converted rather than dropped. A value that cannot be used for its option
	// is ignored rather than rejected, so a mistyped with() value never panics. Two keys are deliberately not handled here:
	//   - "parameters" is applied separately via SetParametersString in submitGremlinLangWithBuilder;
	//     setting it here would trigger the SetParameters/SetParametersString mutual-exclusion panic.
	//   - "g" is not accepted as a with() override by the HTTP API; the traversal source is set
	//     through the client's TraversalSource.
	for key, value := range config {
		switch key {
		case "timeoutMillis":
			if n, ok := toInt(value); ok {
				builder.SetTimeoutMillis(n)
			}
		case "batchSize":
			if n, ok := toInt(value); ok {
				builder.SetBatchSize(n)
			}
		case "materializeProperties":
			if s, ok := value.(string); ok {
				builder.SetMaterializeProperties(s)
			}
		case "bulkResults":
			if b, ok := value.(bool); ok {
				builder.SetBulkResults(b)
			}
		case "language":
			if s, ok := value.(string); ok {
				builder.SetLanguage(s)
			}
		}
	}

	return builder
}

// toInt converts the numeric types that can arrive through with() (values are raw interface{}) into an int, so
// an option supplied as, for example, an int64 or float64 is accepted rather than dropped. It reports false for
// non-numeric values and for values that cannot be represented exactly as an int: non-finite or fractional
// floats and integers outside the int range.
func toInt(value interface{}) (int, bool) {
	switch n := value.(type) {
	case int:
		return n, true
	case int8:
		return int(n), true
	case int16:
		return int(n), true
	case int32:
		return int(n), true
	case int64:
		if n < math.MinInt || n > math.MaxInt {
			return 0, false
		}
		return int(n), true
	case uint:
		return uintToInt(uint64(n))
	case uint8:
		return int(n), true
	case uint16:
		return int(n), true
	case uint32:
		return uintToInt(uint64(n))
	case uint64:
		return uintToInt(n)
	case float32:
		return floatToInt(float64(n))
	case float64:
		return floatToInt(n)
	default:
		return 0, false
	}
}

func uintToInt(n uint64) (int, bool) {
	if n > uint64(math.MaxInt) {
		return 0, false
	}
	return int(n), true
}

func floatToInt(f float64) (int, bool) {
	// -float64(math.MinInt) is 2^63 (2^31 on 32-bit platforms), the first value past the int range
	if math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) ||
		f < float64(math.MinInt) || f >= -float64(math.MinInt) {
		return 0, false
	}
	return int(f), true
}
