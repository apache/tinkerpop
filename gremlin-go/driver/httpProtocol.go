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
	"fmt"
	"net/http"
)

// responsible for serializing and sending requests and then receiving and deserializing responses
type httpProtocol struct {
	serializer   serializer
	logHandler   *logHandler
	url          string
	connSettings *connectionSettings
	httpClient   *http.Client
}

func newHttpProtocol(handler *logHandler, url string, connSettings *connectionSettings) *httpProtocol {
	transport := &http.Transport{
		TLSClientConfig:    connSettings.tlsConfig,
		MaxConnsPerHost:    0, // TODO
		IdleConnTimeout:    0, // TODO
		DisableCompression: !connSettings.enableCompression,
	}

	httpClient := http.Client{
		Transport: transport,
		Timeout:   connSettings.connectionTimeout,
	}

	httpProt := &httpProtocol{
		serializer:   newGraphBinarySerializer(handler),
		logHandler:   handler,
		url:          url,
		connSettings: connSettings,
		httpClient:   &httpClient,
	}
	return httpProt
}

// sends a query request and returns a ResultSet that can be used to obtain query results
func (protocol *httpProtocol) send(request *request) (ResultSet, error) {
	rs := newChannelResultSet()
	fmt.Println("Serializing request")
	bytes, err := protocol.serializer.serializeMessage(request)
	if err != nil {
		rs.setError(err)
		rs.Close()
		return rs, err
	}

	// one transport per request
	transport := NewHttpTransporter(protocol.url, protocol.connSettings, protocol.httpClient, protocol.logHandler)

	// async send request
	transport.wg.Add(1)
	go func() {
		defer transport.wg.Done()
		err := transport.Write(bytes)
		if err != nil {
			rs.setError(err)
			rs.Close()
		}
	}()

	// async receive response
	transport.wg.Add(1)
	go func() {
		defer transport.wg.Done()
		msg, err := transport.Read()
		if err != nil {
			rs.setError(err)
			rs.Close()
		} else {
			err = protocol.receive(rs, msg)
		}
		transport.Close()
	}()

	return rs, err
}

// receives a binary response message, deserializes, and adds results to the ResultSet
func (protocol *httpProtocol) receive(rs ResultSet, msg []byte) error {
	fmt.Println("Deserializing response")
	resp, err := protocol.serializer.deserializeMessage(msg)
	if err != nil {
		protocol.logHandler.logf(Error, logErrorGeneric, "receive()", err.Error())
		rs.Close()
		return err
	}

	fmt.Println("Handling response")
	err = protocol.handleResponse(rs, resp)
	if err != nil {
		protocol.logHandler.logf(Error, logErrorGeneric, "receive()", err.Error())
		rs.Close()
		return err
	}
	return nil
}

// processes a deserialized response and attempts to add results to the ResultSet
func (protocol *httpProtocol) handleResponse(rs ResultSet, response response) error {
	fmt.Println("Handling response")

	statusCode, data := response.responseStatus.code,
		response.responseResult.data
	if rs == nil {
		return newError(err0501ResponseHandlerResultSetNotCreatedError)
	}

	if statusCode == http.StatusNoContent {
		rs.addResult(&Result{make([]interface{}, 0)})
		rs.Close()
		protocol.logHandler.logf(Debug, readComplete)
	} else if statusCode == http.StatusOK {
		rs.addResult(&Result{data})
		rs.Close()
		protocol.logHandler.logf(Debug, readComplete)
	} else if statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden {
		rs.Close()
		err := newError(err0503ResponseHandlerAuthError, response.responseStatus, response.responseResult)
		rs.setError(err)
		return err
	} else {
		rs.Close()
		err := newError(err0502ResponseHandlerReadLoopError, response.responseStatus, statusCode)
		rs.setError(err)
		return err
	}
	return nil
}

func (protocol *httpProtocol) close() {
	protocol.httpClient.CloseIdleConnections()
}
