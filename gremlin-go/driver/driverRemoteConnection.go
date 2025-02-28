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

// DriverRemoteConnectionSettings are used to configure the DriverRemoteConnection.
type DriverRemoteConnectionSettings struct {
	TraversalSource          string
	LogVerbosity             LogVerbosity
	Logger                   Logger
	Language                 language.Tag
	AuthInfo                 AuthInfoProvider
	TlsConfig                *tls.Config
	KeepAliveInterval        time.Duration
	WriteDeadline            time.Duration
	ConnectionTimeout        time.Duration
	EnableCompression        bool
	EnableUserAgentOnConnect bool
	ReadBufferSize           int
	WriteBufferSize          int

	// Minimum amount of concurrent active traversals on a connection to trigger creation of a new connection
	NewConnectionThreshold int
	// Maximum number of concurrent connections. Default: number of runtime processors
	MaximumConcurrentConnections int
	// Initial amount of instantiated connections. Default: 1
	InitialConcurrentConnections int
}

// DriverRemoteConnection is a remote connection.
type DriverRemoteConnection struct {
	client          *Client
	isClosed        bool
	settings        *DriverRemoteConnectionSettings
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
		AuthInfo:                 &AuthInfo{},
		TlsConfig:                &tls.Config{},
		KeepAliveInterval:        keepAliveIntervalDefault,
		WriteDeadline:            writeDeadlineDefault,
		ConnectionTimeout:        connectionTimeoutDefault,
		EnableCompression:        false,
		EnableUserAgentOnConnect: true,
		// ReadBufferSize and WriteBufferSize specify I/O buffer sizes in bytes. The default is 1048576.
		// If a buffer size is set zero, then the default size is used. The I/O buffer
		// sizes do not limit the size of the messages that can be sent or received.
		ReadBufferSize:  1048576,
		WriteBufferSize: 1048576,

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
// TODO test and update when connection is set up
func (driver *DriverRemoteConnection) submitGremlinLang(gremlinLang *GremlinLang) (ResultSet, error) {
	if driver.isClosed {
		return nil, newError(err0203SubmitGremlinLangToClosedConnectionError)
	}
	return driver.client.submitGremlinLang(gremlinLang)
}

// submitBytecode sends a Bytecode traversal to the server.
func (driver *DriverRemoteConnection) submitBytecode(bytecode *Bytecode) (ResultSet, error) {
	if driver.isClosed {
		return nil, newError(err0203SubmitBytecodeToClosedConnectionError)
	}
	return driver.client.submitBytecode(bytecode)
}

func (driver *DriverRemoteConnection) commit() (ResultSet, error) {
	bc := &Bytecode{}
	bc.AddSource("tx", "commit")
	return driver.submitBytecode(bc)
}

func (driver *DriverRemoteConnection) rollback() (ResultSet, error) {
	bc := &Bytecode{}
	bc.AddSource("tx", "rollback")
	return driver.submitBytecode(bc)
}
