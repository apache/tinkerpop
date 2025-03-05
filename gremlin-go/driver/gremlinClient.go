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
)

// responsible for serializing and sending requests and then receiving and deserializing responses
type gremlinClient struct {
	serializer   serializer
	logHandler   *logHandler
	url          string
	connSettings *connectionSettings
	httpClient   *http.Client
}

func newGremlinClient(handler *logHandler, url string, connSettings *connectionSettings) *gremlinClient {
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

	httpProt := &gremlinClient{
		serializer:   newGraphBinarySerializer(handler),
		logHandler:   handler,
		url:          url,
		connSettings: connSettings,
		httpClient:   &httpClient,
	}
	return httpProt
}

// sends a query request and returns a ResultSet that can be used to obtain query results
func (client *gremlinClient) send(request *request) (ResultSet, error) {
	rs := newChannelResultSet()
	bytes, err := client.serializer.serializeMessage(request)
	if err != nil {
		rs.setError(err)
		rs.Close()
		return rs, err
	}

	// one transport per request
	transport := NewHttpTransporter(client.url, client.connSettings, client.httpClient, client.logHandler)

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
			err = client.receive(rs, msg)
		}
		transport.Close()
	}()

	return rs, err
}

// receives a binary response message, deserializes, and adds results to the ResultSet
func (client *gremlinClient) receive(rs ResultSet, msg []byte) error {
	resp, err := client.serializer.deserializeMessage(msg)
	if err != nil {
		client.logHandler.logf(Error, logErrorGeneric, "deserializeMessage()", err.Error())
		rs.Close()
		return err
	}

	err = client.handleResponse(rs, resp)
	if err != nil {
		client.logHandler.logf(Error, logErrorGeneric, "handleResponse()", err.Error())
		rs.Close()
		return err
	}
	return nil
}

// processes a deserialized response and attempts to add results to the ResultSet
func (client *gremlinClient) handleResponse(rs ResultSet, response response) error {
	statusCode, data := response.responseStatus.code, response.responseResult.data
	if rs == nil {
		return newError(err0501ResponseHandlerResultSetNotCreatedError)
	}

	if statusCode == http.StatusNoContent {
		rs.addResult(&Result{make([]interface{}, 0)})
		rs.Close()
		client.logHandler.logf(Debug, readComplete)
	} else if statusCode == http.StatusOK {
		rs.addResult(&Result{data})
		rs.Close()
		client.logHandler.logf(Debug, readComplete)
	} else if statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden {
		rs.Close()
		err := newError(err0503ResponseHandlerAuthError, response.responseStatus, response.responseResult)
		rs.setError(err)
		return err
	} else {
		rs.Close()
		err := newError(err0502ResponseHandlerError, response.responseStatus, statusCode)
		rs.setError(err)
		return err
	}
	return nil
}

func (client *gremlinClient) close() {
	client.httpClient.CloseIdleConnections()
}
