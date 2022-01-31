package gremlingo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

const runIntegration = false

func TestConnection(t *testing.T) {
	t.Run("Test connect", func(t *testing.T) {
		if runIntegration {
			connection := connection{"localhost", 8181, Gorilla, newLogHandler(&defaultLogger{}, Info, language.English), nil, nil, nil}
			err := connection.connect()
			assert.Nil(t, err)
		}
	})

	t.Run("Test write", func(t *testing.T) {
		if runIntegration {
			connection := connection{"localhost", 8181, Gorilla, newLogHandler(&defaultLogger{}, Info, language.English), nil, nil, nil}
			err := connection.connect()
			assert.Nil(t, err)
			request := makeStringRequest("g.V().count()")
			resultSet, err := connection.write(&request)
			assert.Nil(t, err)
			assert.NotNil(t, resultSet)
			result := resultSet.one()
			assert.NotNil(t, result)
			assert.Equal(t, result.AsString(), "[0]")
		}
	})

	t.Run("Test client submit", func(t *testing.T) {
		if runIntegration {
			connection := connection{"localhost", 8181, Gorilla, newLogHandler(&defaultLogger{}, Info, language.English), nil, nil, nil}
			err := connection.connect()
			assert.Nil(t, err)
			client := NewClient("localhost", 8181)
			resultSet, err := client.Submit("g.V().count()")
			assert.Nil(t, err)
			assert.NotNil(t, resultSet)
			result := resultSet.one()
			assert.NotNil(t, result)
			assert.Equal(t, result.AsString(), "[0]")
		}
	})
}
