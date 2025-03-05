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
	"bytes"
	"compress/zlib"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
)

// TODO decide channel size when chunked response handling is implemented - for now just set to 1
const responseChannelSizeDefault = 1

// HttpTransporter responsible for sending and receiving bytes to/from the server
type HttpTransporter struct {
	url             string
	isClosed        bool
	connSettings    *connectionSettings
	responseChannel chan []byte // receives response bytes from the server
	httpClient      *http.Client
	wg              *sync.WaitGroup
	logHandler      *logHandler
}

func NewHttpTransporter(url string, connSettings *connectionSettings, httpClient *http.Client, logHandler *logHandler) *HttpTransporter {
	wg := &sync.WaitGroup{}

	return &HttpTransporter{
		url:             url,
		connSettings:    connSettings,
		responseChannel: make(chan []byte, responseChannelSizeDefault),
		httpClient:      httpClient,
		wg:              wg,
		logHandler:      logHandler,
	}
}

// Write sends bytes to the server as a POST request and sends received response bytes to the responseChannel
func (transporter *HttpTransporter) Write(data []byte) error {
	transporter.logHandler.logf(Debug, creatingRequest)
	req, err := http.NewRequest("POST", transporter.url, bytes.NewBuffer(data))
	if err != nil {
		transporter.logHandler.logf(Error, failedToSendRequest, err.Error())
		return err
	}
	req.Header.Set("content-type", graphBinaryMimeType)
	req.Header.Set("accept", graphBinaryMimeType)
	if transporter.connSettings.enableUserAgentOnConnect {
		req.Header.Set(userAgentHeader, userAgent)
	}
	if transporter.connSettings.enableCompression {
		req.Header.Set("accept-encoding", "deflate")
	}

	transporter.logHandler.logf(Debug, writeRequest)
	resp, err := transporter.httpClient.Do(req)
	if err != nil {
		transporter.logHandler.logf(Error, failedToSendRequest, err.Error())
		return err
	}

	reader := resp.Body
	if resp.Header.Get("content-encoding") == "deflate" {
		reader, err = zlib.NewReader(resp.Body)
		if err != nil {
			transporter.logHandler.logf(Error, failedToReceiveResponse, err.Error())
			return err
		}
	}

	// TODO handle chunked encoding and send chunks to responseChannel
	all, err := io.ReadAll(reader)
	if err != nil {
		transporter.logHandler.logf(Error, failedToReceiveResponse, err.Error())
		return err
	}
	err = reader.Close()
	if err != nil {
		return err
	}

	transporter.logHandler.log(Debug, receivedResponse)

	// possible to receive graph-binary or json error response bodies
	contentType := resp.Header.Get("content-type")
	if resp.StatusCode != 200 && contentType != graphBinaryMimeType {
		if contentType == "application/json" {
			var jsonMap map[string]interface{}
			err = json.Unmarshal(all, &jsonMap)
			if err != nil {
				return err
			}
			message, exists := jsonMap["message"]
			if exists {
				return newError(err0502ResponseHandlerError, message, resp.StatusCode)
			}
			return newError(err0502ResponseHandlerError, "Response was not successful", resp.StatusCode)
		}
		// unexpected error content type
		return newError(err0502ResponseHandlerError, "Response was not successful and of unexpected content-type: "+contentType, resp.StatusCode)
	}

	transporter.responseChannel <- all
	return nil
}

// Read reads bytes from the responseChannel
func (transporter *HttpTransporter) Read() ([]byte, error) {
	msg, ok := <-transporter.responseChannel
	if !ok {
		return []byte{}, errors.New("failed to read from response channel")
	}
	return msg, nil
}

// Close closes the transporter and its corresponding responseChannel
func (transporter *HttpTransporter) Close() {
	if !transporter.isClosed {
		if transporter.responseChannel != nil {
			close(transporter.responseChannel)
		}
		transporter.isClosed = true
	}
}
