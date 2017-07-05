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

package org.apache.tinkerpop.gremlin.python.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.DefaultGremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinJythonScriptEngineTest {

    @Test
    public void shouldGetEngineByName() throws Exception {
        final ScriptEngine engine = new DefaultGremlinScriptEngineManager().getEngineByName("gremlin-jython");
        assertNotNull(engine);
        assertTrue(engine instanceof GremlinJythonScriptEngine);
        assertEquals(3, engine.eval("1+2"));
    }

    @Test
    public void shouldHaveCoreImports() throws Exception {
        final ScriptEngine engine = new DefaultGremlinScriptEngineManager().getEngineByName("gremlin-jython");
        assertThat(engine.eval("Graph"), instanceOf(Class.class));
        assertThat(engine.eval("__"), instanceOf(Class.class));
        assertThat(engine.eval("T"), instanceOf(Class.class));
        assertThat(engine.eval("label"), instanceOf(T.class));
        assertThat(engine.eval("T.label"), instanceOf(T.class));
        assertEquals(SackFunctions.Barrier.class, engine.eval("Barrier"));
        assertEquals(SackFunctions.Barrier.normSack, engine.eval("Barrier.normSack"));
        assertEquals(Column.class, engine.eval("Column"));
        assertEquals(Column.values, engine.eval("Column.valueOf(\'values\')"));
        assertEquals(VertexProperty.Cardinality.class, engine.eval("Cardinality"));
        assertEquals(VertexProperty.Cardinality.single, engine.eval("Cardinality.valueOf(\'single\')"));
        assertTrue(engine.eval("out()") instanceof GraphTraversal);
        assertTrue(engine.eval("__.out()") instanceof GraphTraversal);
        assertTrue(engine.eval("__.property(VertexProperty.Cardinality.single, 'name','marko')") instanceof GraphTraversal);
        assertTrue(engine.eval("__.property(Cardinality.single, 'name','marko')") instanceof GraphTraversal);
    }


    @Test
    public void shouldSupportJavaBasedGraphTraversal() throws Exception {
        final ScriptEngine engine = new DefaultGremlinScriptEngineManager().getEngineByName("gremlin-jython");
        engine.getBindings(ScriptContext.ENGINE_SCOPE).put("graph", TinkerFactory.createModern());
        engine.eval("g = graph.traversal()");
        assertEquals(new HashSet<>(Arrays.asList("ripple", "lop")), engine.eval("g.V().repeat(out()).times(2).values('name').toSet()"));
        assertEquals(new HashSet<>(Arrays.asList("ripple", "lop")), engine.eval("g.V().repeat(__.out()).times(2).values('name').toSet()"));
        assertEquals(new HashSet<>(Arrays.asList("ripple", "lop")), engine.eval("g.V().repeat(out()).times(2).name.toSet()"));
        assertEquals(new HashSet<>(Arrays.asList("ripple", "lop")), engine.eval("g.V().repeat(__.out()).times(2).name.toSet()"));
        assertEquals(new HashSet<>(Arrays.asList("ripple", "lop")), engine.eval("g.V().repeat(__.out()).times(2)[0:2].name.toSet()"));
        assertEquals(new HashSet<>(Arrays.asList("ripple", "lop")), engine.eval("g.V().repeat(__.out()).times(2).name[0:3].toSet()"));
    }

    @Test
    public void shouldSupportSugarMethods() throws Exception {
        final ScriptEngine engine = new DefaultGremlinScriptEngineManager().getEngineByName("gremlin-jython");
        engine.getBindings(ScriptContext.ENGINE_SCOPE).put("graph", TinkerFactory.createModern());
        engine.eval("g = graph.traversal()");
        assertEquals(new HashSet<>(Arrays.asList("ripple", "lop")), engine.eval("g.V().repeat(__.out()).times(2)[0:2].name.toSet()"));
        assertEquals(new HashSet<>(Arrays.asList("ripple", "lop")), engine.eval("g.V().repeat(__.out()).times(2).name[0:3].toSet()"));
        assertEquals(BigInteger.valueOf(1), engine.eval("g.V().repeat(__.out()).times(2).name[0:1].count().next()"));
        assertEquals(BigInteger.valueOf(1), engine.eval("g.V().repeat(__.out()).times(2).name[0].count().next()"));
        assertEquals(BigInteger.valueOf(0), engine.eval("g.V().repeat(__.out()).times(2).name[3].count().next()"));
    }
}
