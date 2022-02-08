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
)

type protocol interface {
	connectionMade(transport transporter)
	read(resultSets map[string]ResultSet) (string, error)
	write(request *request, results map[string]ResultSet) (string, error)
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
}

func (protocol *protocolBase) connectionMade(transporter transporter) {
	protocol.transporter = transporter
}

func (protocol *gremlinServerWSProtocol) read(resultSets map[string]ResultSet) (string, error) {
	// Read data from transport layer.
	msg, err := protocol.transporter.Read()
	if err != nil || msg == nil {
		if err != nil {
			return "", err
		}
		protocol.logHandler.log(Error, malformedURL)
		return "", errors.New("malformed ws or wss URL")
	}
	// Deserialize message and unpack.
	response, err := protocol.serializer.deserializeMessage(msg)
	if err != nil {
		return "", err
	}

	responseID, statusCode, metadata, data := response.responseID, response.responseStatus.code,
		response.responseResult.meta, response.responseResult.data

	resultSet := resultSets[responseID.String()]
	if resultSet == nil {
		resultSet = newChannelResultSet(responseID.String())
	}
	resultSets[responseID.String()] = resultSet
	if aggregateTo, ok := metadata["aggregateTo"]; ok {
		resultSet.setAggregateTo(aggregateTo.(string))
	}

	// Handle status codes appropriately. If status code is http.StatusPartialContent, we need to re-read data.
	if statusCode == http.StatusProxyAuthRequired {
		// TODO AN-989: Implement authentication (including handshaking).
		resultSet.Close()
		return responseID.String(), errors.New("authentication is not currently supported")
	} else if statusCode == http.StatusNoContent {
		// Add empty slice to result.
		resultSet.addResult(&Result{make([]interface{}, 0)})
		resultSet.Close()
		return responseID.String(), nil
	} else if statusCode == http.StatusOK || statusCode == http.StatusPartialContent {
		// Add data to the ResultSet.
		resultSet.addResult(&Result{data})
		if statusCode == http.StatusOK {
			resultSet.setStatusAttributes(response.responseStatus.attributes)
		}

		// More data coming, need to read again.
		if statusCode == http.StatusPartialContent {
			return protocol.read(resultSets)
		}
		resultSet.Close()
		return responseID.String(), nil
	} else {
		resultSet.Close()
		return "", errors.New(fmt.Sprint("statusCode: ", statusCode))
	}
}

func (protocol *gremlinServerWSProtocol) write(request *request, results map[string]ResultSet) (string, error) {
	bytes, err := protocol.serializer.serializeMessage(request)
	if err != nil {
		return "", err
	}
	err = protocol.transporter.Write(bytes)
	if err != nil {
		return "", err
	}
	results[request.requestID.String()] = newChannelResultSet(request.requestID.String())
	go protocol.read(results)
	return request.requestID.String(), nil
}

func newGremlinServerWSProtocol(handler *logHandler) protocol {
	return &gremlinServerWSProtocol{&protocolBase{}, newGraphBinarySerializer(handler), handler, 1, "", ""}
}
