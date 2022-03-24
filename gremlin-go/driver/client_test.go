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
