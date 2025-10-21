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
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const keepAliveIntervalDefault = 5 * time.Second
const writeDeadlineDefault = 3 * time.Second
const writeChannelSizeDefault = 100
const connectionTimeoutDefault = 5 * time.Second

// ReadBufferSize and WriteBufferSize specify I/O buffer sizes in bytes. The default is 1048576.
// If a buffer size is set zero, then the Gorilla websocket 4096 default size is used. The I/O buffer
// sizes do not limit the size of the messages that can be sent or received.
const readBufferSizeDefault = 1048576
const writeBufferSizeDefault = 1048576

// Transport layer that uses gorilla/websocket: https://github.com/gorilla/websocket
// Gorilla WebSocket is a widely used and stable Go implementation of the WebSocket protocol.
type gorillaTransporter struct {
	url          string
	connection   websocketConn
	isClosed     bool
	logHandler   *logHandler
	connSettings *connectionSettings
	writeChannel chan []byte
	wg           *sync.WaitGroup
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

	dialer := &websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  transporter.connSettings.connectionTimeout,
		TLSClientConfig:   transporter.connSettings.tlsConfig,
		EnableCompression: transporter.connSettings.enableCompression,
		ReadBufferSize:    transporter.connSettings.readBufferSize,
		WriteBufferSize:   transporter.connSettings.writeBufferSize,
	}

	header := transporter.getAuthInfo().GetHeader()
	if transporter.connSettings.enableUserAgentOnConnect {
		if header == nil {
			header = make(http.Header)
		}
		header.Set(userAgentHeader, userAgent)
	}

	// Nil is accepted as a valid header, so it can always be passed directly through.
	conn, _, err := dialer.Dial(u.String(), header)
	if err != nil {
		return err
	}
	transporter.connection = conn
	transporter.connection.SetPongHandler(func(string) error {
		err := transporter.connection.SetReadDeadline(time.Now().Add(2 * transporter.connSettings.keepAliveInterval))
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
	// Only validate buffer size if writeBufferSize is explicitly set (> 0)
	// When writeBufferSize is 0, let Gorilla WebSocket handle it with its own defaults
	if transporter.connSettings.writeBufferSize > 0 && len(data) > transporter.connSettings.writeBufferSize {
		return newError(err1201RequestSizeExceedsWriteBufferError)
	}
	transporter.writeChannel <- data
	return nil
}

func (transporter *gorillaTransporter) getAuthInfo() AuthInfoProvider {
	if transporter.connSettings.authInfo == nil {
		return NoopAuthInfo
	}
	return transporter.connSettings.authInfo
}

// Read used to read data from the transporter. Opens connection if closed.
func (transporter *gorillaTransporter) Read() ([]byte, error) {
	if transporter.connection == nil {
		err := transporter.Connect()
		if err != nil {
			return nil, err
		}
	}

	err := transporter.connection.SetReadDeadline(time.Now().Add(transporter.connSettings.keepAliveInterval * 2))
	if err != nil {
		return nil, err
	}
	_, bytes, err := transporter.connection.ReadMessage()
	return bytes, err

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
	defer transporter.wg.Done()

	ticker := time.NewTicker(transporter.connSettings.keepAliveInterval)
	defer ticker.Stop()

	for {
		select {
		case message, ok := <-transporter.writeChannel:
			if !ok {
				// Channel was closed, we can disconnect and exit.
				return
			}

			// Set write deadline.
			err := transporter.connection.SetWriteDeadline(time.Now().Add(transporter.connSettings.writeDeadline))
			if err != nil {
				transporter.logHandler.logf(Error, failedToSetWriteDeadline, err.Error())
				return
			}

			// Write binary message that was submitted to channel.
			err = transporter.connection.WriteMessage(websocket.BinaryMessage, message)
			if err != nil {
				transporter.logHandler.logf(Error, failedToWriteMessage, "BinaryMessage", err.Error())
				return
			}
		case <-ticker.C:
			// Set write deadline.
			err := transporter.connection.SetWriteDeadline(time.Now().Add(transporter.connSettings.keepAliveInterval))
			if err != nil {
				transporter.logHandler.logf(Error, failedToSetWriteDeadline, err.Error())
				return
			}

			// Write pong message.
			err = transporter.connection.WriteMessage(websocket.PingMessage, nil)
			if err != nil {
				transporter.logHandler.logf(Error, failedToWriteMessage, "PingMessage", err.Error())
				return
			}
		}
	}
}
