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
	"sync"
	"time"
)

type connectionSettings struct {
	authInfo                 AuthInfoProvider
	tlsConfig                *tls.Config
	keepAliveInterval        time.Duration
	writeDeadline            time.Duration
	connectionTimeout        time.Duration
	enableCompression        bool
	readBufferSize           int
	writeBufferSize          int
	enableUserAgentOnConnect bool
}

type synchronizedMap struct {
	internalMap map[string]ResultSet
	syncLock    sync.Mutex
}

func (s *synchronizedMap) store(key string, value ResultSet) {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	s.internalMap[key] = value
}

func (s *synchronizedMap) load(key string) ResultSet {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	return s.internalMap[key]
}

func (s *synchronizedMap) delete(key string) {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	delete(s.internalMap, key)
}

func (s *synchronizedMap) size() int {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	return len(s.internalMap)
}

func (s *synchronizedMap) closeAll(err error) {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	for _, resultSet := range s.internalMap {
		resultSet.setError(err)
		resultSet.unlockedClose()
	}
}
