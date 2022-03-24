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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClient(t *testing.T) {
	// Integration test variables.
	testNoAuthUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", "ws://localhost:8182/gremlin")
	testNoAuthEnable := getEnvOrDefaultBool("RUN_INTEGRATION_TESTS", true)
	testNoAuthAuthInfo := &AuthInfo{}
	testNoAuthTlsConfig := &tls.Config{}

	t.Run("Test client.submit()", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.TlsConfig = testNoAuthTlsConfig
				settings.AuthInfo = testNoAuthAuthInfo
			})
		assert.Nil(t, err)
		assert.NotNil(t, client)
		resultSet, err := client.Submit("g.V().count()")
		assert.Nil(t, err)
		assert.NotNil(t, resultSet)
		result, err := resultSet.one()
		assert.Nil(t, err)
		assert.NotNil(t, result)
		err = client.Close()
		assert.Nil(t, err)
	})
}
