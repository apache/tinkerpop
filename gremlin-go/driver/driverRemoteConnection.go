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
	"time"

	"golang.org/x/text/language"
)

// DriverRemoteConnectionSettings are used to configure the DriverRemoteConnection.
type DriverRemoteConnectionSettings struct {
	TraversalSource          string
	LogVerbosity             LogVerbosity
	Logger                   Logger
	Language                 language.Tag
	TlsConfig                *tls.Config
	ConnectionTimeout        time.Duration
	EnableCompression        bool
	EnableUserAgentOnConnect bool

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

	// RequestInterceptors are functions that modify HTTP requests before sending.
	RequestInterceptors []RequestInterceptor
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
