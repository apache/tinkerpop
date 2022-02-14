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
)

type connectionState int

const (
	initialized connectionState = iota
	established
	closed
	closedDueToError
)

type connection struct {
	logHandler *logHandler
	protocol   protocol
	results    map[string]ResultSet
	state      connectionState
}

func (connection *connection) errorCallback() {
	connection.logHandler.log(Error, errorCallback)
	connection.state = closedDueToError
	_ = connection.protocol.close()
}

func (connection *connection) close() (err error) {
	if connection.state != established {
		return errors.New("cannot close connection that has already been closed or has not been connected")
	}
	connection.logHandler.log(Info, closeConnection)
	if connection.protocol != nil {
		err = connection.protocol.close()
	}
	connection.state = closed
	return
}

func (connection *connection) write(request *request) (ResultSet, error) {
	connection.logHandler.log(Info, writeRequest)
	requestID := request.requestID.String()
	connection.logHandler.logf(Info, creatingRequest, requestID)
	connection.results[requestID] = newChannelResultSet(requestID)
	return connection.results[requestID], connection.protocol.write(request)
}

func createConnection(host string, port int, logHandler *logHandler) (*connection, error) {
	conn := &connection{logHandler, nil, map[string]ResultSet{}, initialized}
	logHandler.log(Info, connectConnection)
	protocol, err := newGremlinServerWSProtocol(logHandler, Gorilla, host, port, conn.results, conn.errorCallback)
	conn.protocol = protocol
	if err == nil {
		conn.state = established
		return conn, err
	} else {
		logHandler.logf(Error, failedConnection)
		conn.state = closedDueToError
		return nil, err
	}
}
