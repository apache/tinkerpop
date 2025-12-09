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
	"net/http"
	"sync"
)

// protocol handles invoking serialization and deserialization, as well as handling the lifecycle of raw data passed to
// and received from the transport layer.
type protocol interface {
	readLoop(resultSets *synchronizedMap, errorCallback func())
	write(request *request) error
	close(wait bool) error
}

const authenticationFailed = uint16(151)

type protocolBase struct {
	protocol

	transporter transporter
}

type gremlinServerWSProtocol struct {
	*protocolBase

	serializer Serializer
	logHandler *logHandler
	closed     bool
	mutex      sync.Mutex
	wg         *sync.WaitGroup
}

func (protocol *gremlinServerWSProtocol) readLoop(resultSets *synchronizedMap, errorCallback func()) {
	defer protocol.wg.Done()

	for {
		// Read from transport layer. If the channel is closed, this will error out and exit.
		msg, err := protocol.transporter.Read()
		protocol.mutex.Lock()
		if protocol.closed {
			protocol.mutex.Unlock()
			return
		}
		protocol.mutex.Unlock()
		if err != nil {
			// Ignore error here, we already got an error on read, cannot do anything with this.
			_ = protocol.transporter.Close()
			protocol.logHandler.logf(Error, readLoopError, err.Error())
			readErrorHandler(resultSets, errorCallback, err, protocol.logHandler)
			return
		}

		// Deserialize message and unpack.
		resp, err := protocol.serializer.DeserializeMessage(msg)
		if err != nil {
			protocol.logHandler.logf(Error, logErrorGeneric, "gremlinServerWSProtocol.readLoop()", err.Error())
			readErrorHandler(resultSets, errorCallback, err, protocol.logHandler)
			return
		}

		err = protocol.responseHandler(resultSets, resp)
		if err != nil {
			readErrorHandler(resultSets, errorCallback, err, protocol.logHandler)
			return
		}
	}
}

// If there is an error, we need to close the ResultSets and then pass the error back.
func readErrorHandler(resultSets *synchronizedMap, errorCallback func(), err error, log *logHandler) {
	log.logf(Error, readLoopError, err.Error())
	resultSets.closeAll(err)
	errorCallback()
}

func (protocol *gremlinServerWSProtocol) responseHandler(resultSets *synchronizedMap, response Response) error {
	responseID, statusCode, metadata, data := response.ResponseID, response.ResponseStatus.code,
		response.ResponseResult.Meta, response.ResponseResult.Data
	responseIDString := responseID.String()
	if resultSets.load(responseIDString) == nil {
		return newError(err0501ResponseHandlerResultSetNotCreatedError)
	}
	if aggregateTo, ok := metadata["aggregateTo"]; ok {
		resultSets.load(responseIDString).setAggregateTo(aggregateTo.(string))
	}

	// Handle status codes appropriately. If status code is http.StatusPartialContent, we need to re-read data.
	if statusCode == http.StatusNoContent {
		resultSets.load(responseIDString).addResult(&Result{make([]interface{}, 0)})
		resultSets.load(responseIDString).Close()
		protocol.logHandler.logf(Debug, readComplete, responseIDString)
	} else if statusCode == http.StatusOK {
		// Add data and status attributes to the ResultSet.
		resultSets.load(responseIDString).addResult(&Result{data})
		resultSets.load(responseIDString).setStatusAttributes(response.ResponseStatus.attributes)
		resultSets.load(responseIDString).Close()
		protocol.logHandler.logf(Debug, readComplete, responseIDString)
	} else if statusCode == http.StatusPartialContent {
		// Add data to the ResultSet.
		resultSets.load(responseIDString).addResult(&Result{data})
	} else if statusCode == http.StatusProxyAuthRequired || statusCode == authenticationFailed {
		// http status code 151 is not defined here, but corresponds with 403, i.e. authentication has failed.
		// Server has requested basic auth.
		authInfo := protocol.transporter.getAuthInfo()
		if ok, username, password := authInfo.GetBasicAuth(); ok {
			authBytes := make([]byte, 0)
			authBytes = append(authBytes, 0)
			authBytes = append(authBytes, []byte(username)...)
			authBytes = append(authBytes, 0)
			authBytes = append(authBytes, []byte(password)...)
			encoded := base64.StdEncoding.EncodeToString(authBytes)
			request := makeBasicAuthRequest(encoded)
			err := protocol.write(&request)
			if err != nil {
				return err
			}
		} else {
			resultSets.load(responseIDString).Close()
			return newError(err0503ResponseHandlerAuthError, response.ResponseStatus, response.ResponseResult)
		}
	} else {
		newError := newError(err0502ResponseHandlerReadLoopError, response.ResponseStatus, statusCode)
		resultSets.load(responseIDString).setError(newError)
		resultSets.load(responseIDString).Close()
		protocol.logHandler.logf(Error, logErrorGeneric, "gremlinServerWSProtocol.responseHandler()", newError.Error())
	}
	return nil
}

func (protocol *gremlinServerWSProtocol) write(request *request) error {
	bytes, err := protocol.serializer.SerializeMessage(request)
	if err != nil {
		return err
	}
	return protocol.transporter.Write(bytes)
}

func (protocol *gremlinServerWSProtocol) close(wait bool) error {
	var err error

	protocol.mutex.Lock()
	if !protocol.closed {
		err = protocol.transporter.Close()
		protocol.closed = true
	}
	protocol.mutex.Unlock()

	if wait {
		protocol.wg.Wait()
	}

	return err
}

func newGremlinServerWSProtocol(handler *logHandler, transporterType TransporterType, url string, connSettings *connectionSettings, results *synchronizedMap,
	errorCallback func()) (protocol, error) {
	wg := &sync.WaitGroup{}
	transport, err := getTransportLayer(transporterType, url, connSettings, handler)
	if err != nil {
		return nil, err
	}

	gremlinProtocol := &gremlinServerWSProtocol{
		protocolBase: &protocolBase{transporter: transport},
		serializer:   newGraphBinarySerializer(handler),
		logHandler:   handler,
		closed:       false,
		mutex:        sync.Mutex{},
		wg:           wg,
	}
	err = gremlinProtocol.transporter.Connect()
	if err != nil {
		return nil, err
	}
	wg.Add(1)
	go gremlinProtocol.readLoop(results, errorCallback)
	return gremlinProtocol, nil
}
