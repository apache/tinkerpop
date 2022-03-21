/*
Licensed to the Apache Software Foundation (ASF) Under one
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

func TestStrategy(t *testing.T) {
	testNoAuthUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", "ws://localhost:8182/gremlin")

	t.Run("Test read with AdjacentToIncidentStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(AdjacentToIncidentStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with ByModulatorOptimizationStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(ByModulatorOptimizationStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with CountStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(CountStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with EarlyLimitStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(EarlyLimitStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with FilterRankingStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(FilterRankingStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with IdentityRemovalStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(IdentityRemovalStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with IncidentToAdjacentStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(IncidentToAdjacentStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with InlineFilterStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(InlineFilterStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with LazyBarrierStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(LazyBarrierStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with MatchPredicateStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(MatchPredicateStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with OrderLimitStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(OrderLimitStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with PathProcessorStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(PathProcessorStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with PathRetractionStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(PathRetractionStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with ProductiveByStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(ProductiveByStrategy([]string{"a", "b"})).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with RepeatUnrollStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(RepeatUnrollStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with EdgeLabelVerificationStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(EdgeLabelVerificationStrategy(true, true)).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with LambdaRestrictionStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(LambdaRestrictionStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with TestReadOnlyStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(ReadOnlyStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test write with TestReadOnlyStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		_, promise, err := g.WithStrategies(ReadOnlyStrategy()).AddV("person").Property("name", "foo").Iterate()

		assert.Nil(t, err)
		assert.NotNil(t, <-promise)
	})

	t.Run("Test read with ReservedKeysVerificationStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		strategy := ReservedKeysVerificationStrategy(true, true, []string{"xyz"})
		count, err := g.WithStrategies(strategy).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})

	t.Run("Test read with RepeatUnrollStrategy", func(t *testing.T) {
		g := initializeGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})

		count, err := g.WithStrategies(RepeatUnrollStrategy()).V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Equal(t, int32(6), val)
	})
}
