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
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
)

const authenticationFailed = uint32(151)

type httpProtocol struct {
	serializer   serializer
	logHandler   *logHandler
	url          string
	connSettings *connectionSettings
	httpClient   *http.Client
}

func newHttpProtocol(handler *logHandler, url string, connSettings *connectionSettings) (*httpProtocol, error) {
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
	return httpProt, nil
}

func (protocol *httpProtocol) send(request *request) (ResultSet, error) {
	rs := newChannelResultSet(request.requestID.String())

	fmt.Println("Serializing request")
	bytes, err := protocol.serializer.serializeMessage(request)
	if err != nil {
		return nil, err
	}

	transport := NewHttpTransporter(protocol.url, protocol.connSettings, protocol.httpClient)

	// async send request and wait for response
	transport.wg.Add(1)
	go func() {
		defer transport.wg.Done()
		err := transport.Write(bytes)
		if err != nil {
			rs.setError(err)
			rs.Close()
			protocol.errorCallback()
			err = transport.Close()
		}
	}()

	// async receive response msg data
	transport.wg.Add(1)
	go func() {
		defer transport.wg.Done()
		msg, err := transport.Read()
		if err != nil {
			rs.setError(err)
			rs.Close()
			protocol.errorCallback()
			err = transport.Close()
		} else {
			protocol.receive(rs, msg, protocol.errorCallback)
		}
		rs.Close()
		err = transport.Close()
	}()

	return rs, err
}

func (protocol *httpProtocol) receive(rs ResultSet, msg []byte, errorCallback func()) {
	fmt.Println("Deserializing response")
	resp, err := protocol.serializer.deserializeMessage(msg)
	if err != nil {
		protocol.logHandler.logf(Error, logErrorGeneric, "receive()", err.Error())
		rs.Close()
		return
	}

	fmt.Println("Handling response")
	err = protocol.handleResponse(rs, resp)
	if err != nil {
		protocol.logHandler.logf(Error, logErrorGeneric, "receive()", err.Error())
		rs.Close()
		errorCallback()
		return
	}
}

func (protocol *httpProtocol) handleResponse(rs ResultSet, response response) error {
	fmt.Println("Handling response")

	// TODO http specific response handling - below is just copy-pasted from web socket implementation for now

	responseID, statusCode, metadata, data := response.responseID, response.responseStatus.code,
		response.responseResult.meta, response.responseResult.data
	responseIDString := responseID.String()
	if rs == nil {
		return newError(err0501ResponseHandlerResultSetNotCreatedError)
	}
	if aggregateTo, ok := metadata["aggregateTo"]; ok {
		rs.setAggregateTo(aggregateTo.(string))
	}

	// Handle status codes appropriately. If status code is http.StatusPartialContent, we need to re-read data.
	if statusCode == http.StatusNoContent {
		rs.addResult(&Result{make([]interface{}, 0)})
		rs.Close()
		protocol.logHandler.logf(Debug, readComplete, responseIDString)
	} else if statusCode == http.StatusOK {
		// Add data and status attributes to the ResultSet.
		rs.addResult(&Result{data})
		rs.setStatusAttributes(response.responseStatus.attributes)
		rs.Close()
		protocol.logHandler.logf(Debug, readComplete, responseIDString)
	} else if statusCode == http.StatusPartialContent {
		// Add data to the ResultSet.
		rs.addResult(&Result{data})
	} else if statusCode == http.StatusProxyAuthRequired || statusCode == authenticationFailed {
		// http status code 151 is not defined here, but corresponds with 403, i.e. authentication has failed.
		// Server has requested basic auth.
		authInfo := protocol.getAuthInfo()
		if ok, username, password := authInfo.GetBasicAuth(); ok {
			authBytes := make([]byte, 0)
			authBytes = append(authBytes, 0)
			authBytes = append(authBytes, []byte(username)...)
			authBytes = append(authBytes, 0)
			authBytes = append(authBytes, []byte(password)...)
			encoded := base64.StdEncoding.EncodeToString(authBytes)
			request := makeBasicAuthRequest(encoded)
			// TODO retry
			_, err := fmt.Fprintf(os.Stdout, "Skipping retry of failed request : %s\n", request.requestID)
			if err != nil {
				return err
			}
		} else {
			rs.Close()
			return newError(err0503ResponseHandlerAuthError, response.responseStatus, response.responseResult)
		}
	} else {
		newError := newError(err0502ResponseHandlerReadLoopError, response.responseStatus, statusCode)
		rs.setError(newError)
		rs.Close()
		protocol.logHandler.logf(Error, logErrorGeneric, "httpProtocol.responseHandler()", newError.Error())
	}
	return nil
}

func (protocol *httpProtocol) getAuthInfo() AuthInfoProvider {
	if protocol.connSettings.authInfo == nil {
		return NoopAuthInfo
	}
	return protocol.connSettings.authInfo
}

func (protocol *httpProtocol) errorCallback() {
	protocol.logHandler.log(Error, errorCallback)
}
