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
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// protocol handles invoking serialization and deserialization, as well as handling the lifecycle of raw data passed to
// and received from the transport layer.
type protocol interface {
	readLoop(resultSets map[string]ResultSet, errorCallback func(), log *logHandler)
	write(request *request) error
	close() (err error)
}

const authenticationFailed = uint16(151)

type protocolBase struct {
	protocol

	transporter transporter
}

type gremlinServerWSProtocol struct {
	*protocolBase

	serializer       serializer
	logHandler       *logHandler
	maxContentLength int
	closed           bool
	mux              sync.Mutex
}

func (protocol *gremlinServerWSProtocol) readLoop(resultSets map[string]ResultSet, errorCallback func(), log *logHandler) {
	for {
		// Read from transport layer. If the channel is closed, this will error out and exit.
		msg, err := protocol.transporter.Read()
		protocol.mux.Lock()
		if protocol.closed {
			protocol.mux.Unlock()
			return
		}
		protocol.mux.Unlock()
		if err != nil {
			// Ignore error here, we already got an error on read, cannot do anything with this.
			_ = protocol.transporter.Close()
			log.logf(Error, readLoopError, err.Error())
			readErrorHandler(resultSets, errorCallback, err, log)
			return
		}

		// Deserialize message and unpack.
		resp, err := protocol.serializer.deserializeMessage(msg)
		if err != nil {
			log.logger.Log(Error, err)
			readErrorHandler(resultSets, errorCallback, err, log)
			return
		}

		err = protocol.responseHandler(resultSets, resp, log)
		if err != nil {
			readErrorHandler(resultSets, errorCallback, err, log)
			return
		}
	}
}

// If there is an error, we need to close the ResultSets and then pass the error back.
func readErrorHandler(resultSets map[string]ResultSet, errorCallback func(), err error, log *logHandler) {
	log.logf(Error, readLoopError, err.Error())
	for _, resultSet := range resultSets {
		resultSet.Close()
	}
	errorCallback()
}

func (protocol *gremlinServerWSProtocol) responseHandler(resultSets map[string]ResultSet, response response, log *logHandler) error {
	responseID, statusCode, metadata, data := response.responseID, response.responseStatus.code,
		response.responseResult.meta, response.responseResult.data
	responseIDString := responseID.String()
	if resultSets[responseIDString] == nil {
		return errors.New("resultSet was not created before data was received")
	}
	if aggregateTo, ok := metadata["aggregateTo"]; ok {
		resultSets[responseIDString].setAggregateTo(aggregateTo.(string))
	}

	// Handle status codes appropriately. If status code is http.StatusPartialContent, we need to re-read data.
	if statusCode == http.StatusNoContent {
		resultSets[responseIDString].addResult(&Result{make([]interface{}, 0)})
		resultSets[responseIDString].Close()
		log.logger.Log(Info, "No content.")
		log.logf(Info, readComplete, responseIDString)
	} else if statusCode == http.StatusOK {
		// Add data and status attributes to the ResultSet.
		resultSets[responseIDString].addResult(&Result{data})
		resultSets[responseIDString].setStatusAttributes(response.responseStatus.attributes)
		resultSets[responseIDString].Close()
		log.logger.Logf(Info, "OK %v===>%v", response.responseStatus, data)
		log.logf(Info, readComplete, responseIDString)
	} else if statusCode == http.StatusPartialContent {
		// Add data to the ResultSet.
		resultSets[responseIDString].addResult(&Result{data})
		log.logger.Logf(Info, "Partial %v===>%v", response.responseStatus, data)
	} else if statusCode == http.StatusProxyAuthRequired || statusCode == authenticationFailed {
		// http status code 151 is not defined here, but corresponds with 403, i.e. authentication has failed.
		// Server has requested basic auth.
		authInfo := protocol.transporter.getAuthInfo()
		if authInfo.getUseBasicAuth() {
			username := []byte(authInfo.Username)
			password := []byte(authInfo.Password)

			authBytes := make([]byte, 0)
			authBytes = append(authBytes, 0)
			authBytes = append(authBytes, username...)
			authBytes = append(authBytes, 0)
			authBytes = append(authBytes, password...)
			encoded := base64.StdEncoding.EncodeToString(authBytes)
			request := makeBasicAuthRequest(encoded)
			err := protocol.write(&request)
			if err != nil {
				return err
			}
		} else {
			resultSets[responseIDString].Close()
			return errors.New(fmt.Sprintf("failed to authenticate %v : %v", response.responseStatus, response.responseResult))
		}
	} else {
		errorMessage := fmt.Sprint("Error in read loop, error message '", response.responseStatus, "'. statusCode: ", statusCode)
		resultSets[responseIDString].setError(errors.New(errorMessage))
		resultSets[responseIDString].Close()
		log.logger.Log(Info, errorMessage)
	}
	return nil
}

func (protocol *gremlinServerWSProtocol) write(request *request) error {
	bytes, err := protocol.serializer.serializeMessage(request)
	if err != nil {
		return err
	}
	return protocol.transporter.Write(bytes)
}

func (protocol *gremlinServerWSProtocol) close() (err error) {
	protocol.mux.Lock()
	if !protocol.closed {
		err = protocol.transporter.Close()
		protocol.closed = true
	}
	protocol.mux.Unlock()
	return
}

func newGremlinServerWSProtocol(handler *logHandler, transporterType TransporterType, url string, authInfo *AuthInfo, tlsConfig *tls.Config, results map[string]ResultSet, errorCallback func(), keepAliveInterval time.Duration, writeDeadline time.Duration) (protocol, error) {
	transport, err := getTransportLayer(transporterType, url, authInfo, tlsConfig, keepAliveInterval, writeDeadline)
	if err != nil {
		return nil, err
	}

	gremlinProtocol := &gremlinServerWSProtocol{
		protocolBase:     &protocolBase{transporter: transport},
		serializer:       newGraphBinarySerializer(handler),
		logHandler:       handler,
		maxContentLength: 1,
		closed:           false,
		mux:              sync.Mutex{},
	}
	err = gremlinProtocol.transporter.Connect()
	if err != nil {
		return nil, err
	}
	go gremlinProtocol.readLoop(results, errorCallback, handler)
	return gremlinProtocol, nil
}
