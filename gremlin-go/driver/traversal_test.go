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
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTraversal(t *testing.T) {

	t.Run("Test clone traversal", func(t *testing.T) {
		g := cloneGraphTraversalSource(&Graph{}, NewGremlinLang(nil), nil)
		original := g.V().Out("created")
		clone := original.Clone().Out("knows")
		cloneClone := clone.Clone().Out("created")

		assert.Equal(t, "g.V().out(\"created\")", original.GremlinLang.GetGremlin())
		assert.Equal(t, "g.V().out(\"created\").out(\"knows\")", clone.GremlinLang.GetGremlin())
		assert.Equal(t, "g.V().out(\"created\").out(\"knows\").out(\"created\")", cloneClone.GremlinLang.GetGremlin())

		original.Has("person", "name", "marko")
		clone.V().Out()

		assert.Equal(t, "g.V().out(\"created\").has(\"person\",\"name\",\"marko\")", original.GremlinLang.GetGremlin())
		assert.Equal(t, "g.V().out(\"created\").out(\"knows\").V().out()", clone.GremlinLang.GetGremlin())
		assert.Equal(t, "g.V().out(\"created\").out(\"knows\").out(\"created\")", cloneClone.GremlinLang.GetGremlin())
	})

	t.Run("Test Iterate with empty removeConnection", func(t *testing.T) {
		g := NewGraphTraversalSource(&Graph{}, nil, NewGremlinLang(nil))

		promise := g.V().Count().Iterate()
		assert.NotNil(t, <-promise)
	})

	t.Run("Test WithOptions.Tokens WithOptions.None", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))

		// Get GraphTraversalSource for WithOptions.
		g := newWithOptionsConnection(t)

		results, err := g.V().HasLabel("person").Has("name", "marko").ValueMap("name").With(WithOptions.Tokens, WithOptions.None).ToList()
		assert.Nil(t, err)
		assert.Equal(t, 1, len(results))
		name, ok := results[0].GetInterface().(map[interface{}]interface{})["name"]
		assert.True(t, ok)
		assert.NotNil(t, name)
		assert.Equal(t, 1, len(results[0].GetInterface().(map[interface{}]interface{})))
	})

	t.Run("Test WithOptions.Tokens WithOptions.Ids", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))

		// Get GraphTraversalSource for WithOptions.
		g := newWithOptionsConnection(t)

		results, err := g.V().HasLabel("person").Has("name", "marko").Properties().ValueMap().With(WithOptions.Tokens, WithOptions.Ids).ToList()
		assert.Nil(t, err)
		assert.True(t, len(results) > 0)

		// Expect each result to contain a id. No additional items.
		for _, result := range results {
			key, ok := result.GetInterface().(map[interface{}]interface{})[T.Id]
			assert.True(t, ok)
			assert.NotNil(t, key)

			// Size should be 1.
			assert.Equal(t, 1, len(result.GetInterface().(map[interface{}]interface{})))
		}
	})

	t.Run("Test WithOptions.Tokens WithOptions.Keys", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))

		// Get GraphTraversalSource for WithOptions.
		g := newWithOptionsConnection(t)

		results, err := g.V().HasLabel("person").Has("name", "marko").Properties().ValueMap().With(WithOptions.Tokens, WithOptions.Keys).ToList()
		assert.Nil(t, err)
		assert.True(t, len(results) > 0)

		// Expect each result to contain a key. No additional items.
		for _, result := range results {
			key, ok := result.GetInterface().(map[interface{}]interface{})[T.Key]
			assert.True(t, ok)
			assert.NotNil(t, key)

			// Size should be 1.
			assert.Equal(t, 1, len(result.GetInterface().(map[interface{}]interface{})))
		}
	})

	t.Run("Test WithOptions.Tokens WithOptions.Values", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))

		// Get GraphTraversalSource for WithOptions.
		g := newWithOptionsConnection(t)

		results, err := g.V().HasLabel("person").Has("name", "marko").Properties().ValueMap().With(WithOptions.Tokens, WithOptions.Values).ToList()
		assert.Nil(t, err)
		assert.True(t, len(results) > 0)

		// Expect each result to contain a value. No additional items.
		for _, result := range results {
			key, ok := result.GetInterface().(map[interface{}]interface{})[T.Value]
			assert.True(t, ok)
			assert.NotNil(t, key)

			// Size should be 1.
			assert.Equal(t, 1, len(result.GetInterface().(map[interface{}]interface{})))
		}
	})

	t.Run("Test WithOptions.Tokens WithOptions.All", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))

		// Get GraphTraversalSource for WithOptions.
		g := newWithOptionsConnection(t)

		results, err := g.V().HasLabel("person").Has("name", "marko").Properties().ValueMap().With(WithOptions.Tokens, WithOptions.All).ToList()
		assert.Nil(t, err)
		assert.True(t, len(results) > 0)

		// Expect each result to contain an id, key, and value. No additional items.
		for _, result := range results {
			id, ok := result.GetInterface().(map[interface{}]interface{})[T.Id]
			assert.True(t, ok)
			assert.NotNil(t, id)

			key, ok := result.GetInterface().(map[interface{}]interface{})[T.Key]
			assert.True(t, ok)
			assert.NotNil(t, key)

			value, ok := result.GetInterface().(map[interface{}]interface{})[T.Value]
			assert.True(t, ok)
			assert.NotNil(t, value)

			// Size should be 3.
			assert.Equal(t, 3, len(result.GetInterface().(map[interface{}]interface{})))
		}
	})

	t.Run("Test WithOptions.Indexer WithOptions.List", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))

		// Get GraphTraversalSource for WithOptions.
		g := newWithOptionsConnection(t)

		// We expect our ResultSet to be a single Result that contains a list (array or slice).
		results, err := g.V().HasLabel("person").Values("name").Fold().Index().With(WithOptions.Indexer, WithOptions.List).ToList()
		assert.Nil(t, err)
		assert.Equal(t, 1, len(results))
		assert.True(t, results[0].GetType().Kind() == reflect.Array || results[0].GetType().Kind() == reflect.Slice)
	})

	t.Run("Test WithOptions.Indexer WithOptions.Map", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))

		// Get GraphTraversalSource for WithOptions.
		g := newWithOptionsConnection(t)

		// We expect our ResultSet to be a single Result that contains a map.
		results, err := g.V().HasLabel("person").Values("name").Fold().Index().With(WithOptions.Indexer, WithOptions.Map).ToList()
		assert.Nil(t, err)
		assert.Equal(t, 1, len(results))
		assert.True(t, results[0].GetType().Kind() == reflect.Map)

	})

	t.Run("Test should extract ID from Vertex", func(t *testing.T) {
		g := cloneGraphTraversalSource(&Graph{}, NewGremlinLang(nil), nil)

		// Test basic V() step with mixed ID types
		vStart := g.V(1, &Vertex{Element: Element{Id: 2}})
		assert.Equal(t, "g.V(1,2)", vStart.GremlinLang.GetGremlin())

		// Test V() step in the middle of a traversal
		vMid := g.Inject("foo").V(1, &Vertex{Element: Element{Id: 2}})
		assert.Equal(t, "g.inject(\"foo\").V(1,2)", vMid.GremlinLang.GetGremlin())

		// Test edge creation with from/to vertices
		fromTo := g.AddE("Edge").From(&Vertex{Element: Element{Id: 1}}).To(&Vertex{Element: Element{Id: 2}})
		assert.Equal(t, "g.addE(\"Edge\").from(__.V(1)).to(__.V(2))", fromTo.GremlinLang.GetGremlin())

		// Test mergeE() with Vertex in map
		mergeMap := map[interface{}]interface{}{
			T.Label:       "knows",
			Direction.Out: &Vertex{Element: Element{Id: 1}},
			Direction.In:  &Vertex{Element: Element{Id: 2}},
		}

		mergeEStart := g.MergeE(mergeMap)
		// No order guarantee in map arguments when creating GremlinLang, assert individually
		assert.True(t, strings.HasPrefix(mergeEStart.GremlinLang.GetGremlin(), "g.mergeE("))
		assert.Contains(t, mergeEStart.GremlinLang.GetGremlin(), "label:\"knows\"")
		assert.Contains(t, mergeEStart.GremlinLang.GetGremlin(), "Direction.OUT:1")
		assert.Contains(t, mergeEStart.GremlinLang.GetGremlin(), "Direction.IN:2")

		// Test mergeE() in the middle of a traversal
		mergeEMid := g.Inject("foo").MergeE(mergeMap)
		// No order guarantee in map arguments when creating GremlinLang, assert individually
		assert.True(t, strings.HasPrefix(mergeEMid.GremlinLang.GetGremlin(), "g.inject(\"foo\").mergeE("))
		assert.Contains(t, mergeEMid.GremlinLang.GetGremlin(), "label:\"knows\"")
		assert.Contains(t, mergeEMid.GremlinLang.GetGremlin(), "Direction.OUT:1")
		assert.Contains(t, mergeEMid.GremlinLang.GetGremlin(), "Direction.IN:2")
	})
}

func TestTraversalNextValue(t *testing.T) {
	// Helper to create a closed ResultSet pre-populated with results.
	makeResultSet := func(results ...*Result) ResultSet {
		rs := newChannelResultSetCapacity(len(results) + 1).(*channelResultSet)
		for _, r := range results {
			rs.channel <- r
		}
		rs.channelMutex.Lock()
		rs.closed = true
		close(rs.channel)
		rs.channelMutex.Unlock()
		return rs
	}

	t.Run("unrolls Traverser with bulk > 1", func(t *testing.T) {
		rs := makeResultSet(
			&Result{&Traverser{Bulk: 3, Value: "marko"}},
		)
		trav := &Traversal{results: rs}

		var values []interface{}
		for {
			val, ok, err := trav.nextValue()
			assert.Nil(t, err)
			if !ok {
				break
			}
			values = append(values, val)
		}
		assert.Equal(t, []interface{}{"marko", "marko", "marko"}, values)
	})

	t.Run("unrolls Traverser with bulk == 1", func(t *testing.T) {
		rs := makeResultSet(
			&Result{&Traverser{Bulk: 1, Value: 42}},
		)
		trav := &Traversal{results: rs}

		val, ok, err := trav.nextValue()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, 42, val)

		// Should be exhausted
		_, ok, err = trav.nextValue()
		assert.Nil(t, err)
		assert.False(t, ok)
	})

	t.Run("handles raw non-Traverser results", func(t *testing.T) {
		rs := makeResultSet(
			&Result{"hello"},
			&Result{int32(99)},
		)
		trav := &Traversal{results: rs}

		val, ok, err := trav.nextValue()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, "hello", val)

		val, ok, err = trav.nextValue()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, int32(99), val)

		_, ok, err = trav.nextValue()
		assert.Nil(t, err)
		assert.False(t, ok)
	})

	t.Run("skips Traverser with bulk == 0", func(t *testing.T) {
		rs := makeResultSet(
			&Result{&Traverser{Bulk: 0, Value: "skip-me"}},
			&Result{&Traverser{Bulk: 1, Value: "keep-me"}},
		)
		trav := &Traversal{results: rs}

		val, ok, err := trav.nextValue()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, "keep-me", val)

		_, ok, err = trav.nextValue()
		assert.Nil(t, err)
		assert.False(t, ok)
	})

	t.Run("empty ResultSet returns not-ok", func(t *testing.T) {
		rs := makeResultSet()
		trav := &Traversal{results: rs}

		_, ok, err := trav.nextValue()
		assert.Nil(t, err)
		assert.False(t, ok)
	})

	t.Run("HasNext returns true when lastTraverser has remaining bulk", func(t *testing.T) {
		rs := makeResultSet(
			&Result{&Traverser{Bulk: 3, Value: "x"}},
		)
		trav := &Traversal{results: rs}

		// Consume first value to set lastTraverser
		val, ok, err := trav.nextValue()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, "x", val)

		// HasNext should return true from lastTraverser (bulk=2 remaining)
		hasNext, err := trav.HasNext()
		assert.Nil(t, err)
		assert.True(t, hasNext)

		// Drain remaining
		trav.nextValue() // bulk 2->1
		trav.nextValue() // bulk 1->0, lastTraverser cleared

		// Now should be empty
		hasNext, err = trav.HasNext()
		assert.Nil(t, err)
		assert.False(t, hasNext)
	})
}

func TestTraversalNextN(t *testing.T) {
	makeResultSet := func(results ...*Result) ResultSet {
		rs := newChannelResultSetCapacity(len(results) + 1).(*channelResultSet)
		for _, r := range results {
			rs.channel <- r
		}
		rs.channelMutex.Lock()
		rs.closed = true
		close(rs.channel)
		rs.channelMutex.Unlock()
		return rs
	}

	t.Run("returns exactly n when n is less than available", func(t *testing.T) {
		rs := makeResultSet(&Result{"a"}, &Result{"b"}, &Result{"c"}, &Result{"d"})
		trav := &Traversal{results: rs}

		got, err := trav.NextN(3)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(got))
		assert.Equal(t, "a", got[0].Data)
		assert.Equal(t, "b", got[1].Data)
		assert.Equal(t, "c", got[2].Data)
	})

	t.Run("returns exactly n when n equals available", func(t *testing.T) {
		rs := makeResultSet(&Result{"a"}, &Result{"b"})
		trav := &Traversal{results: rs}

		got, err := trav.NextN(2)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(got))
	})

	t.Run("returns all available when n exceeds available", func(t *testing.T) {
		rs := makeResultSet(&Result{"a"}, &Result{"b"})
		trav := &Traversal{results: rs}

		got, err := trav.NextN(5)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(got))
		assert.Equal(t, "a", got[0].Data)
		assert.Equal(t, "b", got[1].Data)
	})

	t.Run("returns empty slice when n is zero", func(t *testing.T) {
		rs := makeResultSet(&Result{"a"})
		trav := &Traversal{results: rs}

		got, err := trav.NextN(0)
		assert.Nil(t, err)
		assert.NotNil(t, got)
		assert.Equal(t, 0, len(got))
	})

	t.Run("returns empty slice when n is negative", func(t *testing.T) {
		rs := makeResultSet(&Result{"a"})
		trav := &Traversal{results: rs}

		got, err := trav.NextN(-3)
		assert.Nil(t, err)
		assert.NotNil(t, got)
		assert.Equal(t, 0, len(got))
	})

	t.Run("returns empty slice when traversal is exhausted", func(t *testing.T) {
		rs := makeResultSet()
		trav := &Traversal{results: rs}

		got, err := trav.NextN(3)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(got))
	})

	t.Run("unrolls bulked Traverser across the batch", func(t *testing.T) {
		rs := makeResultSet(&Result{&Traverser{Bulk: 3, Value: "x"}})
		trav := &Traversal{results: rs}

		got, err := trav.NextN(2)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(got))
		assert.Equal(t, "x", got[0].Data)
		assert.Equal(t, "x", got[1].Data)
	})

	t.Run("can be called repeatedly to drain in batches", func(t *testing.T) {
		rs := makeResultSet(&Result{1}, &Result{2}, &Result{3}, &Result{4}, &Result{5})
		trav := &Traversal{results: rs}

		first, err := trav.NextN(2)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(first))

		second, err := trav.NextN(10)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(second))

		third, err := trav.NextN(1)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(third))
	})

	t.Run("propagates error from ResultSet", func(t *testing.T) {
		rs := newChannelResultSetCapacity(1).(*channelResultSet)
		rs.setError(assert.AnError)
		rs.channelMutex.Lock()
		rs.closed = true
		close(rs.channel)
		rs.channelMutex.Unlock()
		trav := &Traversal{results: rs}

		got, err := trav.NextN(5)
		assert.Equal(t, assert.AnError, err)
		assert.Equal(t, 0, len(got))
	})
}

func newWithOptionsConnection(t *testing.T) *GraphTraversalSource {
	// No authentication integration test with graphs loaded and alias configured server
	testNoAuthWithAliasUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	testNoAuthWithAliasTlsConfig := &tls.Config{}

	remote, err := NewDriverRemoteConnection(testNoAuthWithAliasUrl,
		func(settings *DriverRemoteConnectionSettings) {
			settings.TlsConfig = testNoAuthWithAliasTlsConfig
			settings.TraversalSource = "gmodern"
		})
	assert.Nil(t, err)
	assert.NotNil(t, remote)
	return Traversal_().With(remote)
}

func newTestRemoteConnection(t *testing.T) *DriverRemoteConnection {
	testNoAuthWithAliasUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	testNoAuthWithAliasTlsConfig := &tls.Config{}

	remote, err := NewDriverRemoteConnection(testNoAuthWithAliasUrl,
		func(settings *DriverRemoteConnectionSettings) {
			settings.TlsConfig = testNoAuthWithAliasTlsConfig
			settings.TraversalSource = "gtx"
		})
	assert.Nil(t, err)
	assert.NotNil(t, remote)
	return remote
}

func getCount(t *testing.T, g *GraphTraversalSource) int32 {
	count, err := g.V().Count().ToList()
	assert.Nil(t, err)
	assert.NotNil(t, count)
	assert.Equal(t, 1, len(count))
	val, err := count[0].GetInt32()
	assert.Nil(t, err)
	return val
}

func TestProviderDefinedTypeTraversalAPIIntegration(t *testing.T) {
	testNoAuthUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	testNoAuthEnable := getEnvOrDefaultBool("RUN_INTEGRATION_TESTS", true)

	t.Run("raw PDT round-trip via Traversal API", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		remote, err := NewDriverRemoteConnection(testNoAuthUrl,
			func(settings *DriverRemoteConnectionSettings) {
				settings.TlsConfig = &tls.Config{}
				settings.TraversalSource = testServerModernGraphAlias
			})
		require.NoError(t, err)
		defer remote.Close()

		g := Traversal_().With(remote)
		pdt := &ProviderDefinedType{Name: "TestPoint", Fields: map[string]interface{}{"x": int32(1), "y": int32(2)}}

		results, err := g.Inject(pdt).ToList()
		require.NoError(t, err)
		require.Len(t, results, 1)

		result, ok := results[0].GetInterface().(*ProviderDefinedType)
		require.True(t, ok, "expected *ProviderDefinedType, got %T", results[0].GetInterface())
		assert.Equal(t, "TestPoint", result.Name)
		assert.Equal(t, int32(1), result.Fields["x"])
		assert.Equal(t, int32(2), result.Fields["y"])
	})

	t.Run("registry-based round-trip via typed struct", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		registry := NewPDTRegistry()
		registry.RegisterFuncsWithType("RegPoint", reflect.TypeOf(regPoint{}),
			func(fields map[string]interface{}) (interface{}, error) {
				return &regPoint{X: fields["x"].(int32), Y: fields["y"].(int32)}, nil
			},
			func(obj interface{}) (map[string]interface{}, error) {
				p := obj.(regPoint)
				return map[string]interface{}{"x": p.X, "y": p.Y}, nil
			})

		remote, err := NewDriverRemoteConnection(testNoAuthUrl,
			func(settings *DriverRemoteConnectionSettings) {
				settings.TlsConfig = &tls.Config{}
				settings.TraversalSource = testServerModernGraphAlias
				settings.PDTRegistry = registry
			})
		require.NoError(t, err)
		defer remote.Close()

		g := Traversal_().With(remote)
		point := regPoint{X: 5, Y: 10}

		results, err := g.Inject(point).ToList()
		require.NoError(t, err)
		require.Len(t, results, 1)

		result, ok := results[0].GetInterface().(*regPoint)
		require.True(t, ok, "expected *regPoint, got %T", results[0].GetInterface())
		assert.Equal(t, int32(5), result.X)
		assert.Equal(t, int32(10), result.Y)
	})
}

// regPoint is a plain struct used for registry-based tests.
type regPoint struct {
	X int32
	Y int32
}