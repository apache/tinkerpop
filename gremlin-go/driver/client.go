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
	"golang.org/x/text/language"
)

// ClientSettings is used to modify a Client's settings on initialization.
type ClientSettings struct {
	TraversalSource string
	TransporterType TransporterType
	LogVerbosity    LogVerbosity
	Logger          Logger
	Language        language.Tag
	AuthInfo        *AuthInfo
	TlsConfig       *tls.Config
}

// Client is used to connect and interact with a Gremlin-supported server.
type Client struct {
	url             string
	traversalSource string
	logHandler      *logHandler
	transporterType TransporterType
	connection      *connection
}

// NewClient creates a Client and configures it with the given parameters.
func NewClient(url string, configurations ...func(settings *ClientSettings)) (*Client, error) {
	settings := &ClientSettings{
		TraversalSource: "g",
		TransporterType: Gorilla,
		LogVerbosity:    Info,
		Logger:          &defaultLogger{},
		Language:        language.English,
		AuthInfo:        &AuthInfo{},
		TlsConfig:       &tls.Config{},
	}
	for _, configuration := range configurations {
		configuration(settings)
	}

	logHandler := newLogHandler(settings.Logger, settings.LogVerbosity, settings.Language)
	conn, err := createConnection(url, settings.AuthInfo, settings.TlsConfig, logHandler)
	if err != nil {
		return nil, err
	}
	client := &Client{
		url:             url,
		traversalSource: "g",
		logHandler:      logHandler,
		transporterType: settings.TransporterType,
		connection:      conn,
	}
	return client, nil
}

// Close closes the client via connection.
func (client *Client) Close() error {
	return client.connection.close()
}

// Submit submits a Gremlin script to the server and returns a ResultSet.
func (client *Client) Submit(traversalString string) (ResultSet, error) {
	// TODO AN-982: Obtain connection from pool of connections held by the client.
	client.logHandler.logf(Debug, submitStartedString, traversalString)
	request := makeStringRequest(traversalString, client.traversalSource)
	return client.connection.write(&request)
}

// submitBytecode submits bytecode to the server to execute and returns a ResultSet.
func (client *Client) submitBytecode(bytecode *bytecode) (ResultSet, error) {
	client.logHandler.logf(Debug, submitStartedBytecode, *bytecode)
	request := makeBytecodeRequest(bytecode, client.traversalSource)
	return client.connection.write(&request)
}
