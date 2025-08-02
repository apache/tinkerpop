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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

/**
 * User Agent body to be sent in web socket handshake
 * Has the form of:
 * [Application Name] [GLV Name]/[Version] [Language Runtime Version] [OS]/[Version] [CPU Architecture]
 * Note: Go does not have any builtin functionality to detect the OS version, therefore OS version will always be
 * reported as NotAvailable
 */
var userAgent string

const userAgentHeader = "User-Agent"

const gremlinVersion = "3.7.4" // DO NOT MODIFY - Configured automatically by Maven Replacer Plugin

func init() {
	applicationName := "NotAvailable"

	path, err := os.Executable()
	if err == nil {
		applicationName = filepath.Base(path)
	}

	applicationName = strings.ReplaceAll(applicationName, " ", "_")
	runtimeVersion := strings.ReplaceAll(runtime.Version(), " ", "_")
	osName := strings.ReplaceAll(runtime.GOOS, " ", "_")
	architecture := strings.ReplaceAll(runtime.GOARCH, " ", "_")
	userAgent = fmt.Sprintf("%v Gremlin-Go.%v %v %v.NotAvailable %v", applicationName, gremlinVersion, runtimeVersion, osName, architecture)
}
