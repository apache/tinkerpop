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
	"errors"
	"fmt"
	"net/http"
	"sync"
)

type protocol interface {
	readLoop(resultSets map[string]ResultSet, errorCallback func(), log *logHandler)
	write(request *request) error
	close() (err error)
}

type protocolBase struct {
	protocol

	transporter transporter
}

type gremlinServerWSProtocol struct {
	*protocolBase

	serializer       serializer
	logHandler       *logHandler
	maxContentLength int
	username         string
	password         string
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
		response, err := protocol.serializer.deserializeMessage(msg)
		if err != nil {
			log.logger.Log(Error, err)
			readErrorHandler(resultSets, errorCallback, err, log)
			return
		}

		err = responseHandler(resultSets, response)
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

func responseHandler(resultSets map[string]ResultSet, response response) error {
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
	} else if statusCode == http.StatusOK {
		// Add data and status attributes to the ResultSet.
		resultSets[responseIDString].addResult(&Result{data})
		resultSets[responseIDString].setStatusAttributes(response.responseStatus.attributes)
		resultSets[responseIDString].Close()
	} else if statusCode == http.StatusPartialContent {
		// Add data to the ResultSet.
		resultSets[responseIDString].addResult(&Result{data})
	} else if statusCode == http.StatusProxyAuthRequired {
		// TODO AN-989: Implement authentication (including handshaking).
		resultSets[responseIDString].Close()
		return errors.New("authentication is not currently supported")
	} else {
		resultSets[responseIDString].Close()
		return errors.New(fmt.Sprint("statusCode: ", statusCode))
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
	}
	protocol.mux.Unlock()
	return
}

func newGremlinServerWSProtocol(handler *logHandler, transporterType TransporterType, host string, port int, results map[string]ResultSet, errorCallback func()) (protocol, error) {
	transporter, err := getTransportLayer(transporterType, host, port)
	if err != nil {
		return nil, err
	}

	gremlinProtocol := &gremlinServerWSProtocol{
		protocolBase:     &protocolBase{transporter: transporter},
		serializer:       newGraphBinarySerializer(handler),
		logHandler:       handler,
		maxContentLength: 1,
		username:         "",
		password:         "",
		closed:           false,
		mux:              sync.Mutex{},
	}

	go gremlinProtocol.readLoop(results, errorCallback, handler)
	return gremlinProtocol, nil
}
