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
	aggregateTo      string
	statusAttributes map[string]interface{}
	closed           bool
	err              error
	mux              sync.Mutex
}

func (channelResultSet *channelResultSet) GetError() error {
	return channelResultSet.err
}

func (channelResultSet *channelResultSet) setError(err error) {
	channelResultSet.err = err
}

func (channelResultSet *channelResultSet) IsEmpty() bool {
	channelResultSet.mux.Lock()
	defer channelResultSet.mux.Unlock()
	return channelResultSet.closed && len(channelResultSet.channel) == 0
}

func (channelResultSet *channelResultSet) Close() {
	if !channelResultSet.closed {
		channelResultSet.mux.Lock()
		defer channelResultSet.mux.Unlock()
		channelResultSet.closed = true
		close(channelResultSet.channel)
	}
}

func (channelResultSet *channelResultSet) setAggregateTo(val string) {
	channelResultSet.aggregateTo = val
}

func (channelResultSet *channelResultSet) GetAggregateTo() string {
	return channelResultSet.aggregateTo
}

func (channelResultSet *channelResultSet) setStatusAttributes(val map[string]interface{}) {
	channelResultSet.statusAttributes = val
}

func (channelResultSet *channelResultSet) GetStatusAttributes() map[string]interface{} {
	return channelResultSet.statusAttributes
}

func (channelResultSet *channelResultSet) GetRequestID() string {
	return channelResultSet.requestID
}

func (channelResultSet *channelResultSet) Channel() chan *Result {
	return channelResultSet.channel
}

func (channelResultSet *channelResultSet) one() (*Result, error) {
	if channelResultSet.err != nil {
		return nil, channelResultSet.err
	}
	return <-channelResultSet.channel, channelResultSet.err
}

func (channelResultSet *channelResultSet) All() ([]*Result, error) {
	var results []*Result
	if channelResultSet.err != nil {
		return nil, channelResultSet.err
	}
	for result := range channelResultSet.channel {
		results = append(results, result)
	}
	return results, channelResultSet.err
}

func (channelResultSet *channelResultSet) addResult(r *Result) {
	if r.GetType().Kind() == reflect.Array || r.GetType().Kind() == reflect.Slice {
		for _, v := range r.result.([]interface{}) {
			if reflect.TypeOf(v) == reflect.TypeOf(&Traverser{}) {
				channelResultSet.channel <- &Result{(v.(*Traverser)).value}
			} else {
				channelResultSet.channel <- &Result{v}
			}
		}
	} else {
		channelResultSet.channel <- &Result{r.result}
	}
}

func newChannelResultSetCapacity(requestID string, channelSize int) ResultSet {
	return &channelResultSet{make(chan *Result, channelSize), requestID, "", nil, false, nil, sync.Mutex{}}
}

func newChannelResultSet(requestID string) ResultSet {
	return newChannelResultSetCapacity(requestID, defaultCapacity)
}
