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
	"reflect"
	"sync"
)

const defaultCapacity = 1000

// ResultSet interface to define the functions of a ResultSet.
type ResultSet interface {
	setAggregateTo(val string)
	GetAggregateTo() string
	setStatusAttributes(statusAttributes map[string]interface{})
	GetStatusAttributes() map[string]interface{}
	GetRequestID() string
	IsEmpty() bool
	Close()
	Channel() chan *Result
	addResult(result *Result)
	one() (*Result, error)
	All() ([]*Result, error)
	GetError() error
	setError(error)
}

// channelResultSet Channel based implementation of ResultSet.
type channelResultSet struct {
	channel          chan *Result
	requestID        string
	container        map[string]ResultSet
	aggregateTo      string
	statusAttributes map[string]interface{}
	closed           bool
	err              error
	waitSignal       chan bool
	channelMutex     sync.Mutex
	waitSignalMutex  sync.Mutex
}

func (channelResultSet *channelResultSet) sendSignal() {
	// Lock wait
	channelResultSet.waitSignalMutex.Lock()
	defer channelResultSet.waitSignalMutex.Unlock()
	if channelResultSet.waitSignal != nil {
		channelResultSet.waitSignal <- true
		channelResultSet.waitSignal = nil
	}
}

// GetError returns error from the channelResultSet.
func (channelResultSet *channelResultSet) GetError() error {
	return channelResultSet.err
}

func (channelResultSet *channelResultSet) setError(err error) {
	channelResultSet.err = err
}

// IsEmpty returns true when the channelResultSet is empty.
func (channelResultSet *channelResultSet) IsEmpty() bool {
	channelResultSet.channelMutex.Lock()
	// If our channel is empty and we have no data in it, wait for signal that the state has been updated.
	if len(channelResultSet.channel) != 0 {
		// Channel is not empty.
		channelResultSet.channelMutex.Unlock()
		return false
	} else if channelResultSet.closed {
		// Channel is empty and closed.
		channelResultSet.channelMutex.Unlock()
		return true
	} else {
		// Channel is empty and not closed. Need to wait for signal that state has changed, otherwise
		// we do not know if it is empty or not.
		// We need to grab the wait signal mutex before we release the channel mutex.
		channelResultSet.waitSignalMutex.Lock()
		channelResultSet.channelMutex.Unlock()

		// Create a wait signal and unlock the wait signal mutex.
		waitSignal := make(chan bool)
		channelResultSet.waitSignal = waitSignal
		channelResultSet.waitSignalMutex.Unlock()

		// Technically if we assigned channelResultSet.waitSignal then unlocked, it could be set to nil or
		// overwritten to another channel before we check it, so to be safe, create additional variable and
		// check that instead.
		<-waitSignal
		return channelResultSet.IsEmpty()
	}
}

// Close can be used to close the channelResultSet.
func (channelResultSet *channelResultSet) Close() {
	if !channelResultSet.closed {
		channelResultSet.channelMutex.Lock()
		channelResultSet.closed = true
		delete(channelResultSet.container, channelResultSet.requestID)
		close(channelResultSet.channel)
		channelResultSet.channelMutex.Unlock()
		channelResultSet.sendSignal()
	}
}

func (channelResultSet *channelResultSet) setAggregateTo(val string) {
	channelResultSet.aggregateTo = val
}

// GetAggregateTo returns aggregateTo for the channelResultSet.
func (channelResultSet *channelResultSet) GetAggregateTo() string {
	return channelResultSet.aggregateTo
}

func (channelResultSet *channelResultSet) setStatusAttributes(val map[string]interface{}) {
	channelResultSet.statusAttributes = val
}

// GetStatusAttributes returns statusAttributes for the channelResultSet.
func (channelResultSet *channelResultSet) GetStatusAttributes() map[string]interface{} {
	return channelResultSet.statusAttributes
}

// GetRequestID returns requestID for the channelResultSet.
func (channelResultSet *channelResultSet) GetRequestID() string {
	return channelResultSet.requestID
}

// Channel returns channel for the channelResultSet.
func (channelResultSet *channelResultSet) Channel() chan *Result {
	return channelResultSet.channel
}

func (channelResultSet *channelResultSet) one() (*Result, error) {
	if channelResultSet.err != nil {
		return nil, channelResultSet.err
	}
	return <-channelResultSet.channel, channelResultSet.err
}

// All returns all results for the channelResultSet.
func (channelResultSet *channelResultSet) All() ([]*Result, error) {
	var results []*Result
	for result := range channelResultSet.channel {
		results = append(results, result)
	}
	return results, channelResultSet.err
}

func (channelResultSet *channelResultSet) addResult(r *Result) {
	channelResultSet.channelMutex.Lock()
	if r.GetType().Kind() == reflect.Array || r.GetType().Kind() == reflect.Slice {
		for _, v := range r.result.([]interface{}) {
			if reflect.TypeOf(v) == reflect.TypeOf(&Traverser{}) {
				for i := int64(0); i < (v.(*Traverser)).bulk; i++ {
					channelResultSet.channel <- &Result{(v.(*Traverser)).value}
				}
			} else {
				channelResultSet.channel <- &Result{v}
			}
		}
	} else {
		channelResultSet.channel <- &Result{r.result}
	}
	channelResultSet.channelMutex.Unlock()
	channelResultSet.sendSignal()
}

func newChannelResultSetCapacity(requestID string, container map[string]ResultSet, channelSize int) ResultSet {
	return &channelResultSet{make(chan *Result, channelSize), requestID, container, "", nil, false, nil, nil, sync.Mutex{}, sync.Mutex{}}
}

func newChannelResultSet(requestID string, container map[string]ResultSet) ResultSet {
	return newChannelResultSetCapacity(requestID, container, defaultCapacity)
}
