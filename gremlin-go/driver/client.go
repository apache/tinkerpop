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
	"runtime"
	"time"

	"golang.org/x/text/language"
)

// ClientSettings is used to modify a Client's settings on initialization.
type ClientSettings struct {
	TraversalSource   string
	TransporterType   TransporterType
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
	Session           string

	// Minimum amount of concurrent active traversals on a connection to trigger creation of a new connection
	NewConnectionThreshold int
	// Maximum number of concurrent connections. Default: number of runtime processors
	MaximumConcurrentConnections int
	// Initial amount of instantiated connections. Default: 1
	InitialConcurrentConnections int
	EnableUserAgentOnConnect     bool
}

// Client is used to connect and interact with a Gremlin-supported server.
type Client struct {
	url             string
	traversalSource string
	logHandler      *logHandler
	transporterType TransporterType
	connections     connectionPool
	session         string
}

// NewClient creates a Client and configures it with the given parameters. During creation of the Client, a connection
// is created, which establishes a websocket.
// Important note: to avoid leaking a connection, always close the Client.
func NewClient(url string, configurations ...func(settings *ClientSettings)) (*Client, error) {
	settings := &ClientSettings{
		TraversalSource:          "g",
		TransporterType:          Gorilla,
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

		NewConnectionThreshold:       defaultNewConnectionThreshold,
		MaximumConcurrentConnections: runtime.NumCPU(),
		InitialConcurrentConnections: defaultInitialConcurrentConnections,
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

	if settings.InitialConcurrentConnections > settings.MaximumConcurrentConnections {
		logHandler.logf(Warning, poolInitialExceedsMaximum, settings.InitialConcurrentConnections,
			settings.MaximumConcurrentConnections, settings.MaximumConcurrentConnections)
		settings.InitialConcurrentConnections = settings.MaximumConcurrentConnections
	}
	pool, err := newLoadBalancingPool(url, logHandler, connSettings, settings.NewConnectionThreshold,
		settings.MaximumConcurrentConnections, settings.InitialConcurrentConnections)
	if err != nil {
		if err != nil {
			logHandler.logf(Error, logErrorGeneric, "NewClient", err.Error())
		}
		return nil, err
	}

	client := &Client{
		url:             url,
		traversalSource: settings.TraversalSource,
		logHandler:      logHandler,
		transporterType: settings.TransporterType,
		connections:     pool,
		session:         settings.Session,
	}

	return client, nil
}

// Close closes the client via connection.
// This is idempotent due to the underlying close() methods being idempotent as well.
func (client *Client) Close() {
	// If it is a session, call closeSession
	if client.session != "" {
		err := client.closeSession()
		if err != nil {
			client.logHandler.logf(Warning, closeSessionRequestError, client.url, client.session, err.Error())
		}
		client.session = ""
	}
	client.logHandler.logf(Info, closeClient, client.url)
	client.connections.close()
}

// SubmitWithOptions submits a Gremlin script to the server with specified RequestOptions and returns a ResultSet.
func (client *Client) SubmitWithOptions(traversalString string, requestOptions RequestOptions) (ResultSet, error) {
	client.logHandler.logf(Debug, submitStartedString, traversalString)
	request := makeStringRequest(traversalString, client.traversalSource, client.session, requestOptions)
	result, err := client.connections.write(&request)
	if err != nil {
		client.logHandler.logf(Error, logErrorGeneric, "Client.Submit()", err.Error())
	}
	return result, err
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

// submitBytecode submits Bytecode to the server to execute and returns a ResultSet.
func (client *Client) submitBytecode(bytecode *Bytecode) (ResultSet, error) {
	client.logHandler.logf(Debug, submitStartedBytecode, *bytecode)
	request := makeBytecodeRequest(bytecode, client.traversalSource, client.session)
	return client.connections.write(&request)
}

func (client *Client) closeSession() error {
	message := makeCloseSessionRequest(client.session)
	result, err := client.connections.write(&message)
	if err != nil {
		return err
	}
	_, err = result.All()
	return err
}
