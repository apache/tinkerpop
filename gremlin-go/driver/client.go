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
	"fmt"
	"golang.org/x/text/language"
	"runtime")

// ClientSettings is used to modify a Client's settings on initialization.
type ClientSettings struct {
	TraversalSource   string
	TransporterType   TransporterType
	LogVerbosity      LogVerbosity
	Logger            Logger
	Language          language.Tag
	AuthInfo          *AuthInfo
	TlsConfig         *tls.Config
	KeepAliveInterval time.Duration
	WriteDeadline     time.Duration
	// Minimum amount of concurrent active traversals on a connection to trigger creation of a new connection
	NewConnectionThreshold int
	// Maximum number of concurrent connections. Default: number of runtime processors
	MaximumConcurrentConnections int
	
	Session         string
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
		TraversalSource:   "g",
		TransporterType:   Gorilla,
		LogVerbosity:      Info,
		Logger:            &defaultLogger{},
		Language:          language.English,
		AuthInfo:          &AuthInfo{},
		TlsConfig:         &tls.Config{},
		KeepAliveInterval: keepAliveIntervalDefault,
		WriteDeadline:     writeDeadlineDefault,
		NewConnectionThreshold:       defaultNewConnectionThreshold,
		MaximumConcurrentConnections: runtime.NumCPU(),
		Session:                      "",
	}
	for _, configuration := range configurations {
		configuration(settings)
	}

	logHandler := newLogHandler(settings.Logger, settings.LogVerbosity, settings.Language)
	if settings.Session != "" {
		logHandler.log(Info, sessionDetected)
		settings.MaximumConcurrentConnections = 1
	}

	pool, err := newLoadBalancingPool(url, settings.AuthInfo, settings.TlsConfig, settings.KeepAliveInterval, settings.WriteDeadline, settings.NewConnectionThreshold,
		settings.MaximumConcurrentConnections, logHandler)
	if err != nil {
		return nil, fmt.Errorf("failed to create client with url '%s' and transport type '%v'. Error message: '%s'",
			url, settings.TransporterType, err.Error())
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
	// If it is a Session, call closeSession
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

// Submit submits a Gremlin script to the server and returns a ResultSet.
func (client *Client) Submit(traversalString string, bindings ...map[string]interface{}) (ResultSet, error) {
	client.logHandler.logf(Debug, submitStartedString, traversalString)
	request := makeStringRequest(traversalString, client.traversalSource, client.session, bindings...)
	return client.connections.write(&request)
	if err != nil {
		client.logHandler.logf(Error, logErrorGeneric, "Client.Submit()", err.Error())
	}
	return result, err
}

// submitBytecode submits bytecode to the server to execute and returns a ResultSet.
func (client *Client) submitBytecode(bytecode *bytecode) (ResultSet, error) {
	client.logHandler.logf(Debug, submitStartedBytecode, *bytecode)
	request := makeBytecodeRequest(bytecode, client.traversalSource, client.session)
	return client.connections.write(&request)
}

func (client *Client) closeSession() error {
	message := makeCloseSessionRequest(client.session)
	_, err := client.connections.write(&message)
	return err
}
