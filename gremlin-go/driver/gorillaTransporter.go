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
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const maxFailCount = 3

const keepAliveIntervalDefault = 5 * time.Second
const writeDeadlineDefault = 3 * time.Second
const writeChannelSizeDefault = 100

// Transport layer that uses gorilla/websocket: https://github.com/gorilla/websocket
// Gorilla WebSocket is a widely used and stable Go implementation of the WebSocket protocol.
type gorillaTransporter struct {
	url               string
	connection        websocketConn
	isClosed          bool
	logHandler        logHandler
	authInfo          *AuthInfo
	tlsConfig         *tls.Config
	keepAliveInterval time.Duration
	writeDeadline     time.Duration
	writeChannel      chan []byte
	wg                *sync.WaitGroup
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
	if err != nil {
		return err
	}
	transporter.connection = conn
	transporter.connection.SetPongHandler(func(string) error {
		err := transporter.connection.SetReadDeadline(time.Now().Add(2 * transporter.keepAliveInterval))
		if err != nil {
			return err
		}
		return nil
	})
	transporter.wg.Add(1)
	go transporter.writeLoop()
	return
}

// Write used to write data to the transporter. Opens connection if closed.
func (transporter *gorillaTransporter) Write(data []byte) error {
	if transporter.connection == nil {
		err := transporter.Connect()
		if err != nil {
			return err
		}
	}
	transporter.writeChannel <- data
	return nil
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
		err := transporter.connection.SetReadDeadline(time.Now().Add(transporter.keepAliveInterval * 2))
		if err != nil {
			return nil, err
		}
		_, bytes, err := transporter.connection.ReadMessage()
		return bytes, err
	}
}

// Close used to close a connection if it is opened.
func (transporter *gorillaTransporter) Close() (err error) {
	if !transporter.isClosed {
		if transporter.writeChannel != nil {
			close(transporter.writeChannel)
		}
		if transporter.wg != nil {
			transporter.wg.Wait()
		}
		err = transporter.connection.Close()
		transporter.isClosed = true
		if err != nil {
			return err
		}
	}
	return
}

// IsClosed returns true when the transporter is closed.
func (transporter *gorillaTransporter) IsClosed() bool {
	return transporter.isClosed
}

func (transporter *gorillaTransporter) writeLoop() {
	ticker := time.NewTicker(transporter.keepAliveInterval)
	defer func() {
		ticker.Stop()
	}()
	for {
		select {
		case message, ok := <-transporter.writeChannel:
			if !ok {
				// Channel was closed, we can disconnect and exit.
				transporter.wg.Done()
				return
			}

			// Set write deadline.
			err := transporter.connection.SetWriteDeadline(time.Now().Add(transporter.writeDeadline))
			if err != nil {
				transporter.logHandler.logf(Error, failedToSetWriteDeadline, err.Error())
				transporter.wg.Done()
				return
			}

			// Write binary message that was submitted to channel.
			err = transporter.connection.WriteMessage(websocket.BinaryMessage, message)
			if err != nil {
				transporter.logHandler.logf(Error, failedToWriteMessage, "BinaryMessage", err.Error())
				transporter.wg.Done()
				return
			}
		case <-ticker.C:
			// Set write deadline.
			err := transporter.connection.SetWriteDeadline(time.Now().Add(transporter.keepAliveInterval))
			if err != nil {
				transporter.logHandler.logf(Error, failedToSetWriteDeadline, err.Error())
				transporter.wg.Done()
				return
			}

			// Write pong message.
			err = transporter.connection.WriteMessage(websocket.PingMessage, nil)
			if err != nil {
				transporter.logHandler.logf(Error, failedToWriteMessage, "PingMessage", err.Error())
				transporter.wg.Done()
				return
			}
		}
	}
}
