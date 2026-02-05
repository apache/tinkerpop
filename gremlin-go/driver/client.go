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
	"reflect"
	"time"

	"golang.org/x/text/language"
)

const connectionTimeoutDefault = 5 * time.Second

// ClientSettings is used to modify a Client's settings on initialization.
type ClientSettings struct {
	TraversalSource   string
	LogVerbosity      LogVerbosity
	Logger            Logger
	Language          language.Tag
	TlsConfig         *tls.Config
	ConnectionTimeout time.Duration
	EnableCompression bool

	// MaximumConcurrentConnections is the maximum number of concurrent TCP connections
	// to the Gremlin server. This limits how many requests can be in-flight simultaneously.
	// Default: 128. Set to 0 to use the default.
	MaximumConcurrentConnections int

	// MaxIdleConnections is the maximum number of idle (keep-alive) connections to retain
	// in the connection pool. Idle connections are reused for subsequent requests.
	// Default: 8. Set to 0 to use the default.
	MaxIdleConnections int

	// IdleConnectionTimeout is how long an idle connection remains in the pool before
	// being closed. Set this to match your server's idle timeout if needed.
	// Default: 180 seconds (3 minutes). Set to 0 to use the default.
	IdleConnectionTimeout time.Duration

	// KeepAliveInterval is the interval between TCP keep-alive probes on idle connections.
	// This helps detect dead connections and keeps connections alive through firewalls.
	// Default: 30 seconds. Set to 0 to use the default.
	KeepAliveInterval time.Duration

	EnableUserAgentOnConnect bool

	// RequestInterceptors are functions that modify HTTP requests before sending.
	RequestInterceptors []RequestInterceptor
}

// Client is used to connect and interact with a Gremlin-supported server.
type Client struct {
	url                string
	traversalSource    string
	logHandler         *logHandler
	connectionSettings *connectionSettings
	conn               *connection
}

// NewClient creates a Client and configures it with the given parameters.
// Important note: to avoid leaking a connection, always close the Client.
func NewClient(url string, configurations ...func(settings *ClientSettings)) (*Client, error) {
	settings := &ClientSettings{
		TraversalSource:          "g",
		LogVerbosity:             Info,
		Logger:                   &defaultLogger{},
		Language:                 language.English,
		TlsConfig:                &tls.Config{},
		ConnectionTimeout:        connectionTimeoutDefault,
		EnableCompression:        false,
		EnableUserAgentOnConnect: true,

		MaximumConcurrentConnections: 0, // Use default (128)
		MaxIdleConnections:           0, // Use default (8)
		IdleConnectionTimeout:        0, // Use default (180s)
		KeepAliveInterval:            0, // Use default (30s)
	}
	for _, configuration := range configurations {
		configuration(settings)
	}

	connSettings := &connectionSettings{
		tlsConfig:                settings.TlsConfig,
		connectionTimeout:        settings.ConnectionTimeout,
		maxConnsPerHost:          settings.MaximumConcurrentConnections,
		maxIdleConnsPerHost:      settings.MaxIdleConnections,
		idleConnTimeout:          settings.IdleConnectionTimeout,
		keepAliveInterval:        settings.KeepAliveInterval,
		enableCompression:        settings.EnableCompression,
		enableUserAgentOnConnect: settings.EnableUserAgentOnConnect,
	}

	logHandler := newLogHandler(settings.Logger, settings.LogVerbosity, settings.Language)

	conn := newConnection(logHandler, url, connSettings)

	// Add user-provided interceptors
	for _, interceptor := range settings.RequestInterceptors {
		conn.AddInterceptor(interceptor)
	}

	client := &Client{
		url:                url,
		traversalSource:    settings.TraversalSource,
		logHandler:         logHandler,
		connectionSettings: connSettings,
		conn:               conn,
	}

	return client, nil
}

// Close closes the client via connection.
// This is idempotent due to the underlying close() methods being idempotent as well.
func (client *Client) Close() {
	client.logHandler.logf(Info, closeClient, client.url)
	if client.conn != nil {
		client.conn.close()
	}
}

func (client *Client) errorCallback() {
	client.logHandler.log(Error, errorCallback)
}

// SubmitWithOptions submits a Gremlin script to the server with specified RequestOptions and returns a ResultSet.
func (client *Client) SubmitWithOptions(traversalString string, requestOptions RequestOptions) (ResultSet, error) {
	client.logHandler.logf(Debug, submitStartedString, traversalString)
	request := MakeStringRequest(traversalString, client.traversalSource, requestOptions)
	rs, err := client.conn.submit(&request)
	return rs, err
}

// Submit submits a Gremlin script to the server and returns a ResultSet. Submit can optionally accept a map of bindings
// to be applied to the traversalString, it is preferred however to instead wrap any bindings into a RequestOptions
// struct and use SubmitWithOptions().
func (client *Client) Submit(traversalString string, bindings ...map[string]interface{}) (ResultSet, error) {
	requestOptionsBuilder := new(RequestOptionsBuilder)
	if len(bindings) > 0 {
		requestOptionsBuilder.SetBindings(bindings[0])
	}
	return client.SubmitWithOptions(traversalString, requestOptionsBuilder.Create())
}

// submitGremlinLang submits GremlinLang to the server to execute and returns a ResultSet.
func (client *Client) submitGremlinLang(gremlinLang *GremlinLang) (ResultSet, error) {
	client.logHandler.logf(Debug, submitStartedString, *gremlinLang)
	requestOptionsBuilder := new(RequestOptionsBuilder)
	if len(gremlinLang.GetParameters()) > 0 {
		requestOptionsBuilder.SetBindings(gremlinLang.GetParameters())
	}
	if len(gremlinLang.optionsStrategies) > 0 {
		requestOptionsBuilder = applyOptionsConfig(requestOptionsBuilder, gremlinLang.optionsStrategies[0].configuration)
	}

	request := MakeStringRequest(gremlinLang.GetGremlin(), client.traversalSource, requestOptionsBuilder.Create())
	return client.conn.submit(&request)
}

func applyOptionsConfig(builder *RequestOptionsBuilder, config map[string]interface{}) *RequestOptionsBuilder {
	builderValue := reflect.ValueOf(builder)

	// Map configuration keys to setter method names
	setterMap := map[string]string{
		"requestId":             "SetRequestId",
		"evaluationTimeout":     "SetEvaluationTimeout",
		"batchSize":             "SetBatchSize",
		"userAgent":             "SetUserAgent",
		"bindings":              "SetBindings",
		"materializeProperties": "SetMaterializeProperties",
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
