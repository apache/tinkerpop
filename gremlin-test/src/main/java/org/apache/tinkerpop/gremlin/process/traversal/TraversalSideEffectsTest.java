/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TraversalSideEffectsTest extends AbstractGremlinProcessTest {
    public abstract TraversalSideEffects get_g_V_asAdmin_getSideEffects();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_sideEffects() {
        final TraversalSideEffects sideEffects = get_g_V_asAdmin_getSideEffects();
        assertFalse(sideEffects.get("a").isPresent());
        assertEquals(StringFactory.traversalSideEffectsString(sideEffects), sideEffects.toString());
    }

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class Traversals extends TraversalSideEffectsTest {
        @Override
        public TraversalSideEffects get_g_V_asAdmin_getSideEffects() {
            return g.V().asAdmin().getSideEffects();
        }
    }
}
