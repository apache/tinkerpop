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

import "net/http"

// AuthInfoProvider is an interface that allows authentication information to be specified.
type AuthInfoProvider interface {
	GetHeader() http.Header
	GetBasicAuth() (ok bool, username, password string)
}

// AuthInfo is an option struct that allows authentication information to be specified statically.
// Authentication can be provided via http.Header directly.
// Basic authentication can also be used via the BasicAuthInfo function.
type AuthInfo struct {
	Header   http.Header
	Username string
	Password string
}

var _ AuthInfoProvider = (*AuthInfo)(nil)

// GetHeader provides a safe way to get a header from the AuthInfo even if it is nil.
// This way we don't need any additional logic in the transport layer.
func (authInfo *AuthInfo) GetHeader() http.Header {
	if authInfo == nil {
		return nil
	} else {
		return authInfo.Header
	}
}

// GetBasicAuth provides a safe way to check if basic auth info is available from the AuthInfo even if it is nil.
// This way we don't need any additional logic in the transport layer.
func (authInfo *AuthInfo) GetBasicAuth() (bool, string, string) {
	if authInfo == nil || (authInfo.Username == "" && authInfo.Password == "") {
		return false, "", ""
	}
	return true, authInfo.Username, authInfo.Password
}

// BasicAuthInfo provides a way to generate AuthInfo. Enter username and password and get the AuthInfo back.
func BasicAuthInfo(username string, password string) *AuthInfo {
	return &AuthInfo{Username: username, Password: password}
}

// HeaderAuthInfo provides a way to generate AuthInfo with only Header information.
func HeaderAuthInfo(header http.Header) *AuthInfo {
	return &AuthInfo{Header: header}
}

// DynamicAuth is an AuthInfoProvider that allows dynamic credential generation.
type DynamicAuth struct {
	fn func() AuthInfoProvider
}

var (
	_ AuthInfoProvider = (*DynamicAuth)(nil)

	// NoopAuthInfo is a no-op AuthInfoProvider that can be used to disable authentication.
	NoopAuthInfo = NewDynamicAuth(func() AuthInfoProvider { return &AuthInfo{} })
)

// NewDynamicAuth provides a way to generate dynamic credentials with the specified generator function.
func NewDynamicAuth(f func() AuthInfoProvider) *DynamicAuth {
	return &DynamicAuth{fn: f}
}

// GetHeader calls the stored function to get the header dynamically.
func (d *DynamicAuth) GetHeader() http.Header {
	return d.fn().GetHeader()
}

// GetBasicAuth calls the stored function to get basic authentication dynamically.
func (d *DynamicAuth) GetBasicAuth() (bool, string, string) {
	return d.fn().GetBasicAuth()
}
