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

import "github.com/google/uuid"

// responseStatus contains the status info of the response.
type responseStatus struct {
	code      uint32
	message   string
	exception string
}

// responseResult contains the result info of the response.
type responseResult struct {
	data interface{}
}

// response represents a response from the server.
type response struct {
	responseID     uuid.UUID
	responseStatus responseStatus
	responseResult responseResult
}
