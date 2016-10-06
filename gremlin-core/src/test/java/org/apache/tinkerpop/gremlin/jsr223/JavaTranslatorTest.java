/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.jsr223;

import org.apache.tinkerpop.gremlin.process.traversal.Bindings;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JavaTranslatorTest {

    @Test
    public void shouldTranslateNestedBindings() {
        final Bindings b = new Bindings();
        final GraphTraversalSource g = EmptyGraph.instance().traversal().withBindings(b);

        final Bytecode bytecode = g.withStrategies(new HashMap<String, Object>() {{
            put(SubgraphStrategy.STRATEGY, SubgraphStrategy.class.getCanonicalName());
            put(SubgraphStrategy.VERTICES, b.of("a", __.has("name", "marko")));
        }}).V().out(b.of("b", "created")).asAdmin().getBytecode();

        Traversal.Admin<?, ?> traversal = JavaTranslator.of(EmptyGraph.instance().traversal()).translate(bytecode);
        assertFalse(traversal.isLocked());
        assertEquals(2, traversal.getSteps().size());
        // assertEquals(traversal.getBytecode(),bytecode); TODO: bindings are removed -- should translated bytecode be the same bytecode?!
        traversal.applyStrategies();
        assertEquals(5, TraversalHelper.getStepsOfAssignableClassRecursively(TraversalFilterStep.class, traversal).size());

        // JavaTranslator should not mutate original bytecode
        assertTrue(bytecode.getBindings().containsKey("a"));
        assertTrue(bytecode.getBindings().containsKey("b"));
        assertEquals(2, bytecode.getBindings().size());
        assertEquals(__.has("name", "marko").asAdmin().getBytecode(), bytecode.getBindings().get("a"));
        assertEquals("created", bytecode.getBindings().get("b"));
    }
}
