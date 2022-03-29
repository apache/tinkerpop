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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const mockMessage string = "MockMessage"
const mockWriteErrMessage string = "MockWriteJSONErrMessage"
const mockReadErrMessage string = "MockReadMessageErrMessage"
const mockCloseErrMessage string = "MockCloseErrMessage"

type mockWebsocketConn struct {
	mock.Mock
}

func (conn *mockWebsocketConn) WriteMessage(messageType int, data []byte) error {
	args := conn.Called(messageType, data)
	return args.Error(0)
}

func (conn *mockWebsocketConn) ReadMessage() (int, []byte, error) {
	args := conn.Called()
	return args.Get(0).(int), args.Get(1).([]byte), args.Error(2)
}

func (conn *mockWebsocketConn) Close() error {
	args := conn.Called()
	return args.Error(0)
}

func (conn *mockWebsocketConn) SetReadDeadline(time time.Time) error {
	args := conn.Called(time)
	return args.Error(0)
}

func (conn *mockWebsocketConn) SetWriteDeadline(time time.Time) error {
	args := conn.Called(time)
	return args.Error(0)
}

func (conn *mockWebsocketConn) SetPongHandler(h func(appData string) error) {
	conn.Called(h)
}

func TestGorillaTransporter(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		mockConn := new(mockWebsocketConn)
		transporter := gorillaTransporter{
			url:          "ws://mockHost:8182/gremlin",
			connection:   mockConn,
			isClosed:     false,
			writeChannel: make(chan []byte, 100),
			wg:           &sync.WaitGroup{},
		}

		t.Run("WriteMessage", func(t *testing.T) {
			mockConn.On("WriteMessage", 2, make([]byte, 10)).Return(nil)
			err := transporter.Write(make([]byte, 10))
			assert.Nil(t, err)
		})

		t.Run("Read", func(t *testing.T) {
			mockConn.On("ReadMessage").Return(0, []byte(mockMessage), nil)
			mockConn.On("SetPongHandler", mock.AnythingOfType("func(string) error")).Return(nil)
			mockConn.On("SetReadDeadline", mock.Anything).Return(nil)
			mockConn.On("SetWriteDeadline", mock.Anything).Return(nil)
			message, err := transporter.Read()
			assert.Nil(t, err)
			assert.Equal(t, mockMessage, string(message[:]))
		})

		t.Run("Close and IsClosed", func(t *testing.T) {
			mockConn.On("Close").Return(nil)
			isClosed := transporter.IsClosed()
			assert.False(t, isClosed)
			err := transporter.Close()
			assert.Nil(t, err)
			isClosed = transporter.IsClosed()
			assert.True(t, isClosed)
		})
	})

	t.Run("Error", func(t *testing.T) {
		mockConn := new(mockWebsocketConn)
		transporter := gorillaTransporter{
			url:          "ws://mockHost:8182/gremlin",
			connection:   mockConn,
			isClosed:     false,
			writeChannel: make(chan []byte, 100),
		}

		t.Run("Read", func(t *testing.T) {
			mockConn.On("ReadMessage").Return(0, []byte{}, errors.New(mockReadErrMessage))
			mockConn.On("SetPongHandler", mock.AnythingOfType("func(string) error")).Return(nil)
			mockConn.On("SetReadDeadline", mock.Anything).Return(nil)
			mockConn.On("SetWriteDeadline", mock.Anything).Return(nil)
			mockConn.On("WriteMessage", mock.Anything, mock.Anything).Return(nil)
			_, err := transporter.Read()
			assert.NotNil(t, err)
			assert.Equal(t, mockReadErrMessage, err.Error())
		})

		t.Run("Close and IsClosed", func(t *testing.T) {
			mockConn.On("Close").Return(nil)
			isClosed := transporter.IsClosed()
			assert.False(t, isClosed)
			err := transporter.Close()
			assert.Nil(t, err)
			isClosed = transporter.IsClosed()
			assert.True(t, isClosed)
		})
	})
}
