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
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

const maxFailCount = 3

// Transport layer that uses gorilla/websocket: https://github.com/gorilla/websocket
// Gorilla WebSocket is a widely used and stable Go implementation of the WebSocket protocol.
type gorillaTransporter struct {
	host       string
	port       int
	connection websocketConn
	isClosed   bool
}

// Connect used to establish a connection.
func (transporter *gorillaTransporter) Connect() (err error) {
	if transporter.connection != nil {
		return
	}

	u := url.URL{
		Scheme: scheme,
		Host:   transporter.host + ":" + strconv.Itoa(transporter.port),
		Path:   path,
	}

	dialer := websocket.DefaultDialer
	// TODO: make this configurable from client; this currently does nothing since 4096 is the default
	dialer.WriteBufferSize = 4096
	conn, _, err := dialer.Dial(u.String(), nil)
	if err == nil {
		transporter.connection = conn
	}
	return
}

// Write used to write data to the transporter. Opens connection if closed.
func (transporter *gorillaTransporter) Write(data []byte) (err error) {
	if transporter.connection == nil {
		err = transporter.Connect()
		if err != nil {
			return
		}
	}

	err = transporter.connection.WriteMessage(websocket.BinaryMessage, data)
	return err
}

// Read used to read data from the transporter. Opens connection if closed.
func (transporter *gorillaTransporter) Read() ([]byte, error) {
	if transporter.connection == nil {
		err := transporter.Connect()
		if err != nil {
			return nil, err
		}
	}
	transporter.connection.SetPongHandler(func(string) error {
		err := transporter.connection.SetReadDeadline(time.Now().Add(time.Second))
		if err != nil {
			return err
		}
		return nil
	})

	err := transporter.connection.SetReadDeadline(time.Now().Add(time.Second))
	if err != nil {
		return nil, err
	}

	failureCount := 0
	for {
		_, bytes, err := transporter.connection.ReadMessage()
		if err == nil {
			return bytes, nil
		}
		failureCount += 1
		if failureCount > maxFailCount {
			return nil, errors.New(fmt.Sprintf("failed to read from socket more than %d times", maxFailCount))
		}

		// Try pinging server.
		err = transporter.connection.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
		if err != nil {
			return nil, err
		}
		err = transporter.connection.WriteMessage(websocket.PingMessage, nil)
		if err != nil {
			return nil, err
		}
	}
}

// Close used to close a connection if it is opened.
func (transporter *gorillaTransporter) Close() (err error) {
	if transporter.connection != nil && !transporter.isClosed {
		transporter.isClosed = true
		return transporter.connection.Close()
	}
	return
}

// IsClosed returns true when the transporter is closed.
func (transporter *gorillaTransporter) IsClosed() bool {
	return transporter.isClosed
}
