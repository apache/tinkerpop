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
	"github.com/google/uuid"
	"golang.org/x/text/language"
	"runtime"
	"time"
)

// DriverRemoteConnectionSettings are used to configure the DriverRemoteConnection.
type DriverRemoteConnectionSettings struct {
	session string

	TraversalSource          string
	TransporterType          TransporterType
	LogVerbosity             LogVerbosity
	Logger                   Logger
	Language                 language.Tag
	AuthInfo                 *AuthInfo
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
	spawnedSessions []*DriverRemoteConnection
	isClosed        bool
	settings        *DriverRemoteConnectionSettings
}

// NewDriverRemoteConnection creates a new DriverRemoteConnection.
// If no custom connection settings are passed in, a connection will be created with "g" as the default TraversalSource,
// Gorilla as the default Transporter, Info as the default LogVerbosity, a default logger struct, and English and as the
// default language
func NewDriverRemoteConnection(
	url string,
	configurations ...func(settings *DriverRemoteConnectionSettings)) (*DriverRemoteConnection, error) {
	settings := &DriverRemoteConnectionSettings{
		session: "",

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
		// ReadBufferSize and WriteBufferSize specify I/O buffer sizes in bytes. The default is 1048576.
		// If a buffer size is set zero, then the Gorilla websocket 4096 default size is used. The I/O buffer
		// sizes do not limit the size of the messages that can be sent or received.
		ReadBufferSize:  1048576,
		WriteBufferSize: 1048576,

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
	if settings.session != "" {
		logHandler.log(Debug, sessionDetected)
		settings.MaximumConcurrentConnections = 1
	}

	if settings.InitialConcurrentConnections > settings.MaximumConcurrentConnections {
		logHandler.logf(Warning, poolInitialExceedsMaximum, settings.InitialConcurrentConnections,
			settings.MaximumConcurrentConnections, settings.MaximumConcurrentConnections)
		settings.InitialConcurrentConnections = settings.MaximumConcurrentConnections
	}
	pool, err := newLoadBalancingPool(url, logHandler, connSettings, settings.NewConnectionThreshold,
		settings.MaximumConcurrentConnections, settings.InitialConcurrentConnections)
	if err != nil {
		if err != nil {
			logHandler.logf(Error, logErrorGeneric, "NewDriverRemoteConnection", err.Error())
		}
		return nil, err
	}

	client := &Client{
		url:             url,
		traversalSource: settings.TraversalSource,
		logHandler:      logHandler,
		transporterType: settings.TransporterType,
		connections:     pool,
		session:         settings.session,
	}

	return &DriverRemoteConnection{client: client, isClosed: false, settings: settings}, nil
}

// Close closes the DriverRemoteConnection.
// Errors if any will be logged
func (driver *DriverRemoteConnection) Close() {
	// If DriverRemoteConnection has spawnedSessions then they must be closed as well.
	if len(driver.spawnedSessions) > 0 {
		driver.client.logHandler.logf(Debug, closingSpawnedSessions, driver.client.url)
		for _, session := range driver.spawnedSessions {
			session.Close()
		}
		driver.spawnedSessions = driver.spawnedSessions[:0]
	}

	if driver.isSession() {
		driver.client.logHandler.logf(Info, closeSession, driver.client.url, driver.client.session)
	} else {
		driver.client.logHandler.logf(Info, closeDriverRemoteConnection, driver.client.url)
	}
	driver.client.Close()
	driver.isClosed = true
}

// Submit sends a string traversal to the server.
func (driver *DriverRemoteConnection) Submit(traversalString string) (ResultSet, error) {
	result, err := driver.client.Submit(traversalString)
	if err != nil {
		driver.client.logHandler.logf(Error, logErrorGeneric, "Driver.Submit()", err.Error())
	}
	return result, err
}

// submitBytecode sends a Bytecode traversal to the server.
func (driver *DriverRemoteConnection) submitBytecode(bytecode *Bytecode) (ResultSet, error) {
	if driver.isClosed {
		return nil, newError(err0203SubmitBytecodeToClosedConnectionError)
	}
	return driver.client.submitBytecode(bytecode)
}

func (driver *DriverRemoteConnection) isSession() bool {
	return driver.client.session != ""
}

// CreateSession generates a new session. sessionId stores the optional UUID param. It can be used to create a session with a specific UUID.
func (driver *DriverRemoteConnection) CreateSession(sessionId ...string) (*DriverRemoteConnection, error) {
	if len(sessionId) > 1 {
		return nil, newError(err0201CreateSessionMultipleIdsError)
	} else if driver.isSession() {
		return nil, newError(err0202CreateSessionFromSessionError)
	}

	driver.client.logHandler.log(Info, creatingSessionConnection)
	drc, err := NewDriverRemoteConnection(driver.client.url, func(settings *DriverRemoteConnectionSettings) {
		if len(sessionId) == 1 {
			settings.session = sessionId[0]
		} else {
			settings.session = uuid.New().String()
		}
		// copy other settings from parent
		settings.TraversalSource = driver.settings.TraversalSource
		settings.TransporterType = driver.settings.TransporterType
		settings.Logger = driver.settings.Logger
		settings.LogVerbosity = driver.settings.LogVerbosity
		settings.Language = driver.settings.Language
		settings.AuthInfo = driver.settings.AuthInfo
		settings.TlsConfig = driver.settings.TlsConfig
		settings.KeepAliveInterval = driver.settings.KeepAliveInterval
		settings.WriteDeadline = driver.settings.WriteDeadline
		settings.ConnectionTimeout = driver.settings.ConnectionTimeout
		settings.NewConnectionThreshold = driver.settings.NewConnectionThreshold
		settings.EnableCompression = driver.settings.EnableCompression
		settings.ReadBufferSize = driver.settings.ReadBufferSize
		settings.WriteBufferSize = driver.settings.WriteBufferSize
		settings.MaximumConcurrentConnections = driver.settings.MaximumConcurrentConnections
	})
	if err != nil {
		return nil, err
	}
	driver.spawnedSessions = append(driver.spawnedSessions, drc)
	return drc, nil
}

func (driver *DriverRemoteConnection) GetSessionId() string {
	return driver.client.session
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
