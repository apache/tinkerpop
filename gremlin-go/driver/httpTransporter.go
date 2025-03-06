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
const contentTypeHeader = "content-type"

// httpTransporter responsible for sending and receiving bytes to/from the server
type httpTransporter struct {
	url             string
	isClosed        bool
	connSettings    *connectionSettings
	responseChannel chan []byte // receives response bytes from the server
	httpClient      *http.Client
	wg              *sync.WaitGroup
	logHandler      *logHandler
}

func newHttpTransporter(url string, connSettings *connectionSettings, httpClient *http.Client, logHandler *logHandler) *httpTransporter {
	wg := &sync.WaitGroup{}

	return &httpTransporter{
		url:             url,
		connSettings:    connSettings,
		responseChannel: make(chan []byte, responseChannelSizeDefault),
		httpClient:      httpClient,
		wg:              wg,
		logHandler:      logHandler,
	}
}

// write sends bytes to the server as a POST request and sends received response bytes to the responseChannel
func (transporter *httpTransporter) write(data []byte) error {
	resp, err := transporter.sendRequest(data)
	if err != nil {
		return err
	}

	respBytes, err := transporter.readResponse(resp)
	if err != nil {
		return err
	}

	// possible to receive graph-binary or json error response bodies
	if resp.StatusCode != 200 && resp.Header.Get(contentTypeHeader) != graphBinaryMimeType {
		return transporter.createResponseError(respBytes, resp)
	}

	transporter.responseChannel <- respBytes
	return nil
}

// read reads bytes from the responseChannel
func (transporter *httpTransporter) read() ([]byte, error) {
	msg, ok := <-transporter.responseChannel
	if !ok {
		return []byte{}, errors.New("failed to read from response channel")
	}
	return msg, nil
}

// close closes the transporter and its corresponding responseChannel
func (transporter *httpTransporter) close() {
	if !transporter.isClosed {
		if transporter.responseChannel != nil {
			close(transporter.responseChannel)
		}
		transporter.isClosed = true
	}
}

func (transporter *httpTransporter) createResponseError(respBytes []byte, resp *http.Response) error {
	contentType := resp.Header.Get(contentTypeHeader)
	if contentType == "application/json" {
		var jsonMap map[string]interface{}
		err := json.Unmarshal(respBytes, &jsonMap)
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

// reads bytes from the given response
func (transporter *httpTransporter) readResponse(resp *http.Response) ([]byte, error) {
	var reader io.ReadCloser
	var err error

	if resp.Header.Get("content-encoding") == "deflate" {
		reader, err = zlib.NewReader(resp.Body)
		if err != nil {
			transporter.logHandler.logf(Error, failedToReceiveResponse, err.Error())
			return nil, err
		}
	} else {
		reader = resp.Body
	}

	// TODO handle chunked encoding and send chunks to responseChannel
	all, err := io.ReadAll(reader)
	if err != nil {
		transporter.logHandler.logf(Error, failedToReceiveResponse, err.Error())
		return nil, err
	}
	err = reader.Close()
	if err != nil {
		return nil, err
	}
	transporter.logHandler.log(Debug, receivedResponse)
	return all, nil
}

// sends a POST request for the given byte content
func (transporter *httpTransporter) sendRequest(data []byte) (*http.Response, error) {
	transporter.logHandler.logf(Debug, creatingRequest)
	req, err := http.NewRequest("POST", transporter.url, bytes.NewBuffer(data))
	if err != nil {
		transporter.logHandler.logf(Error, failedToSendRequest, err.Error())
		return nil, err
	}
	req.Header.Set(contentTypeHeader, graphBinaryMimeType)
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
		return nil, err
	}
	return resp, nil
}
