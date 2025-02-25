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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGraphTraversalSource(t *testing.T) {

	t.Run("GraphTraversalSource.With tests", func(t *testing.T) {
		t.Run("Test for single property", func(t *testing.T) {
			g := &GraphTraversalSource{graph: &Graph{}, gremlinLang: NewGremlinLang(nil), remoteConnection: nil}
			traversal := g.With("foo", "bar")
			assert.NotNil(t, traversal)
			assert.Equal(t, 1, len(traversal.gremlinLang.optionsStrategies))
			config := traversal.gremlinLang.optionsStrategies[0].configuration
			assert.Equal(t, map[string]interface{}{"foo": "bar"}, config)
		})

		t.Run("Test for multiple property", func(t *testing.T) {
			g := &GraphTraversalSource{graph: &Graph{}, gremlinLang: NewGremlinLang(nil), remoteConnection: nil}
			traversal := g.With("foo", "bar").With("foo2", "bar2")
			assert.NotNil(t, traversal)
			assert.Equal(t, 1, len(traversal.gremlinLang.optionsStrategies))
			config := traversal.gremlinLang.optionsStrategies[0].configuration
			assert.Equal(t, map[string]interface{}{"foo": "bar", "foo2": "bar2"}, config)
		})

		t.Run("Test for property replacement", func(t *testing.T) {
			g := &GraphTraversalSource{graph: &Graph{}, gremlinLang: NewGremlinLang(nil), remoteConnection: nil}
			traversal := g.With("foo", "bar").With("foo", "not bar")
			assert.NotNil(t, traversal)
			assert.Equal(t, 1, len(traversal.gremlinLang.optionsStrategies))
			config := traversal.gremlinLang.optionsStrategies[0].configuration
			assert.Equal(t, map[string]interface{}{"foo": "not bar"}, config)
		})
	})
}
