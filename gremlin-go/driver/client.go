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
	"runtime"
	"time"

	"golang.org/x/text/language"
)

const keepAliveIntervalDefault = 5 * time.Second
const writeDeadlineDefault = 3 * time.Second
const connectionTimeoutDefault = 5 * time.Second

// ReadBufferSize and WriteBufferSize specify I/O buffer sizes in bytes. The default is 1MB.
// If a buffer size is set zero, then the transporter default size is used. The I/O buffer
// sizes do not limit the size of the messages that can be sent or received.
const readBufferSizeDefault = 1048576
const writeBufferSizeDefault = 1048576

// ClientSettings is used to modify a Client's settings on initialization.
type ClientSettings struct {
	TraversalSource   string
	LogVerbosity      LogVerbosity
	Logger            Logger
	Language          language.Tag
	AuthInfo          AuthInfoProvider
	TlsConfig         *tls.Config
	KeepAliveInterval time.Duration
	WriteDeadline     time.Duration
	ConnectionTimeout time.Duration
	EnableCompression bool
	ReadBufferSize    int
	WriteBufferSize   int

	// Maximum number of concurrent connections. Default: number of runtime processors
	MaximumConcurrentConnections int
	EnableUserAgentOnConnect     bool
}

// Client is used to connect and interact with a Gremlin-supported server.
type Client struct {
	url                string
	traversalSource    string
	logHandler         *logHandler
	connectionSettings *connectionSettings
	httpProtocol       *httpProtocol
}

// NewClient creates a Client and configures it with the given parameters.
// Important note: to avoid leaking a connection, always close the Client.
func NewClient(url string, configurations ...func(settings *ClientSettings)) (*Client, error) {
	settings := &ClientSettings{
		TraversalSource:          "g",
		LogVerbosity:             Info,
		Logger:                   &defaultLogger{},
		Language:                 language.English,
		AuthInfo:                 &AuthInfo{},
		TlsConfig:                &tls.Config{},
		KeepAliveInterval:        keepAliveIntervalDefault,
		WriteDeadline:            writeDeadlineDefault,
		ConnectionTimeout:        connectionTimeoutDefault,
		EnableCompression:        false,
		EnableUserAgentOnConnect: true,
		ReadBufferSize:           readBufferSizeDefault,
		WriteBufferSize:          writeBufferSizeDefault,

		MaximumConcurrentConnections: runtime.NumCPU(),
	}
	for _, configuration := range configurations {
		configuration(settings)
	}

	connSettings := &connectionSettings{
		authInfo:                 settings.AuthInfo,
		tlsConfig:                settings.TlsConfig,
		keepAliveInterval:        settings.KeepAliveInterval,
		writeDeadline:            settings.WriteDeadline,
		connectionTimeout:        settings.ConnectionTimeout,
		enableCompression:        settings.EnableCompression,
		readBufferSize:           settings.ReadBufferSize,
		writeBufferSize:          settings.WriteBufferSize,
		enableUserAgentOnConnect: settings.EnableUserAgentOnConnect,
	}

	logHandler := newLogHandler(settings.Logger, settings.LogVerbosity, settings.Language)

	httpProt := newHttpProtocol(logHandler, url, connSettings)

	client := &Client{
		url:                url,
		traversalSource:    settings.TraversalSource,
		logHandler:         logHandler,
		connectionSettings: connSettings,
		httpProtocol:       httpProt,
	}

	return client, nil
}

// Close closes the client via connection.
// This is idempotent due to the underlying close() methods being idempotent as well.
func (client *Client) Close() {
	// TODO check what needs to be closed
	client.logHandler.logf(Info, closeClient, client.url)
}

func (client *Client) errorCallback() {
	client.logHandler.log(Error, errorCallback)
}

// SubmitWithOptions submits a Gremlin script to the server with specified RequestOptions and returns a ResultSet.
func (client *Client) SubmitWithOptions(traversalString string, requestOptions RequestOptions) (ResultSet, error) {
	client.logHandler.logf(Debug, submitStartedString, traversalString)
	request := MakeStringRequest(traversalString, client.traversalSource, requestOptions)

	// TODO interceptors (ie. auth)

	rs, err := client.httpProtocol.send(&request)
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
// TODO test and update when connection is set up
func (client *Client) submitGremlinLang(gremlinLang *GremlinLang) (ResultSet, error) {
	client.logHandler.logf(Debug, submitStartedString, *gremlinLang)
	// TODO placeholder
	requestOptionsBuilder := new(RequestOptionsBuilder)
	if len(gremlinLang.GetParameters()) > 0 {
		requestOptionsBuilder.SetBindings(gremlinLang.GetParameters())
	}
	if len(gremlinLang.optionsStrategies) > 0 {
		requestOptionsBuilder = applyOptionsConfig(requestOptionsBuilder, gremlinLang.optionsStrategies[0].configuration)
	}

	request := MakeStringRequest(gremlinLang.GetGremlin(), client.traversalSource, requestOptionsBuilder.Create())
	return client.httpProtocol.send(&request)
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
