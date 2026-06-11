/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.groovy

import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader
import org.apache.tinkerpop.gremlin.groovy.util.SugarTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory
import org.junit.Before
import org.junit.Test

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail

/**
 * Verifies that, with the Gremlin Groovy sugar loaded, the subscript operator (tree[key]) on a
 * {@link Tree} resolves to {@code childAt(key)}, restoring the tree['marko']['josh'] syntax after
 * {@code Tree} stopped extending {@code HashMap}.
 */
class TreeSubscriptTest {

    @Before
    public void setup() throws Exception {
        SugarTestHelper.clearRegistry()
        SugarLoader.load()
    }

    @Test
    public void shouldSupportSubscriptViaSugar() {
        def g = TinkerFactory.createModern().traversal()
        Tree tree = g.V(1).out().out().tree().by('name').next()

        assertEquals(tree.childAt('marko'), tree['marko'])
        assertEquals(tree.childAt('marko').childAt('josh'), tree['marko']['josh'])
        assertTrue(tree['marko']['josh'].rootNodes().contains('ripple'))
        assertTrue(tree['marko']['josh'].rootNodes().contains('lop'))
    }

    @Test
    public void shouldThrowOnMissingSubscriptKey() {
        def g = TinkerFactory.createModern().traversal()
        Tree tree = g.V(1).out().out().tree().by('name').next()

        try {
            tree['nonexistent']
            fail("expected IllegalArgumentException for missing subscript key")
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.message.contains('nonexistent'))
        }
    }
}
