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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

func TestProtocol(t *testing.T) {
	t.Run("Test protocol connect error.", func(t *testing.T) {
		connSettings := newDefaultConnectionSettings()
		connSettings.authInfo, connSettings.tlsConfig = nil, nil
		connSettings.keepAliveInterval, connSettings.writeDeadline, connSettings.writeDeadline = keepAliveIntervalDefault, writeDeadlineDefault, connectionTimeoutDefault

		protocol, err := newGremlinServerWSProtocol(newLogHandler(&defaultLogger{}, Info, language.English), Gorilla,
			"ws://localhost:9000/gremlin", connSettings,
			nil, nil)
		assert.NotNil(t, err)
		assert.Nil(t, protocol)
	})

	t.Run("Test protocol close wait", func(t *testing.T) {
		wg := &sync.WaitGroup{}
		protocol := &gremlinServerWSProtocol{
			closed: true,
			mutex:  sync.Mutex{},
			wg:     wg,
		}
		wg.Add(1)

		done := make(chan bool)

		go func() {
			protocol.close(true)
			done <- true
		}()

		select {
		case <-time.After(1 * time.Second):
			// Ok. Close must wait.
		case <-done:
			t.Fatal("protocol.close is not waiting")
		}
	})

	t.Run("Test protocol close no wait", func(t *testing.T) {
		wg := &sync.WaitGroup{}
		protocol := &gremlinServerWSProtocol{
			closed: true,
			mutex:  sync.Mutex{},
			wg:     wg,
		}
		wg.Add(1)

		done := make(chan bool)

		go func() {
			protocol.close(false)
			done <- true
		}()

		select {
		case <-time.After(1 * time.Second):
			t.Fatal("protocol.close is waiting")
		case <-done:
			// Ok. Close must not wait.
		}
	})
}
