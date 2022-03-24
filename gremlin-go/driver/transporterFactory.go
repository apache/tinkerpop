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
	"errors"
	"sync"
	"time"
)

// TransporterType is an alias for valid transport protocols.
type TransporterType int

const (
	// Gorilla transport layer: github.com/gorilla/websocket
	Gorilla TransporterType = iota + 1
)

func getTransportLayer(transporterType TransporterType, url string, authInfo *AuthInfo, tlsConfig *tls.Config, keepAliveInterval time.Duration, writeDeadline time.Duration) (transporter, error) {
	var transporter transporter
	switch transporterType {
	case Gorilla:
		transporter = &gorillaTransporter{url: url, authInfo: authInfo, tlsConfig: tlsConfig, keepAliveInterval: keepAliveInterval, writeDeadline: writeDeadline, writeChannel: make(chan []byte, writeChannelSizeDefault), wg: &sync.WaitGroup{}}
	default:
		return nil, errors.New("transport layer type was not specified and cannot be initialized")
	}
	err := transporter.Connect()
	if err != nil {
		return nil, err
	}
	return transporter, nil
}
