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
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

const maxFailCount = 3

// Transport layer that uses gorilla/websocket: https://github.com/gorilla/websocket
// Gorilla WebSocket is a widely used and stable Go implementation of the WebSocket protocol.
type gorillaTransporter struct {
	url        string
	connection websocketConn
	isClosed   bool
	authInfo   *AuthInfo
	tlsConfig  *tls.Config
}

// Connect used to establish a connection.
func (transporter *gorillaTransporter) Connect() (err error) {
	if transporter.connection != nil {
		return
	}

	var u *url.URL
	u, err = url.Parse(transporter.url)
	if err != nil {
		return
	}

	dialer := websocket.DefaultDialer
	dialer.TLSClientConfig = transporter.tlsConfig
	// TODO: make this configurable from client; this currently does nothing since 4096 is the default
	dialer.WriteBufferSize = 4096
	// Nil is accepted as a valid header, so it can always be passed directly through.
	conn, _, err := dialer.Dial(u.String(), transporter.authInfo.getHeader())
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
	return
}

func (transporter *gorillaTransporter) getAuthInfo() *AuthInfo {
	return transporter.authInfo
}

// Read used to read data from the transporter. Opens connection if closed.
func (transporter *gorillaTransporter) Read() ([]byte, error) {
	if transporter.connection == nil {
		err := transporter.Connect()
		if err != nil {
			return nil, err
		}
	}

	for {
		err := transporter.connection.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, bytes, err := transporter.connection.ReadMessage()
		return bytes, err
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
