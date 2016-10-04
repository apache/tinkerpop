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

package org.apache.tinkerpop.gremlin.python.structure.io.graphson;

import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.python.jsr223.JythonScriptEngineSetup;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.python.jsr223.PyScriptEngine;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONReaderTest {

    private static final ScriptEngine jythonEngine = JythonScriptEngineSetup.setup((PyScriptEngine) new ScriptEngineManager().getEngineByName("jython"));
    private static final ObjectMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V2_0).create().createMapper();
    private static final GraphTraversalSource g = TinkerFactory.createModern().traversal();


    @Test
    public void shouldDeserializeGraphObjects() throws Exception {
        final Vertex vertex = g.V(1).next();
        jythonEngine.getBindings(ScriptContext.ENGINE_SCOPE).put("x", mapper.writeValueAsString(vertex));
        assertEquals(vertex.toString(), jythonEngine.eval("str(graphson_io.readObject(x))"));
        assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x),Vertex)"));
        //
        final Edge edge = g.V(1).outE("created").next();
        jythonEngine.getBindings(ScriptContext.ENGINE_SCOPE).put("x", mapper.writeValueAsString(edge));
        assertEquals(edge.toString(), jythonEngine.eval("str(graphson_io.readObject(x))"));
        assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x),Edge)"));
        //
        final VertexProperty vertexProperty = (VertexProperty) g.V(1).properties("name").next();
        jythonEngine.getBindings(ScriptContext.ENGINE_SCOPE).put("x", mapper.writeValueAsString(vertexProperty));
        assertEquals(vertexProperty.toString(), jythonEngine.eval("str(graphson_io.readObject(x))"));
        assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x),VertexProperty)"));
        //
        final Property property = g.V(1).outE("created").properties("weight").next();
        jythonEngine.getBindings(ScriptContext.ENGINE_SCOPE).put("x", mapper.writeValueAsString(property));
        assertEquals(property.toString(), jythonEngine.eval("str(graphson_io.readObject(x))"));
        assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x),Property)"));
        //
        final Traverser<Vertex> traverser = new DefaultRemoteTraverser<>(vertex, 3L);
        jythonEngine.getBindings(ScriptContext.ENGINE_SCOPE).put("x", mapper.writeValueAsString(traverser));
        assertEquals(traverser.toString(), jythonEngine.eval("str(graphson_io.readObject(x))"));
        assertEquals(BigInteger.valueOf(3L), jythonEngine.eval("graphson_io.readObject(x).bulk")); // jython uses big integer in Java
        assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x).object,Vertex)"));
        assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x),Traverser)"));
    }

    @Test
    public void shouldDeserializeNumbers() throws Exception {
        jythonEngine.getBindings(ScriptContext.ENGINE_SCOPE).put("x", mapper.writeValueAsString(1));
        assertEquals("1", jythonEngine.eval("str(graphson_io.readObject(x))"));
        assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x),int)"));
        //
        jythonEngine.getBindings(ScriptContext.ENGINE_SCOPE).put("x", mapper.writeValueAsString(1L));
        assertEquals("1", jythonEngine.eval("str(graphson_io.readObject(x))"));
        assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x),long)"));
        //
        jythonEngine.getBindings(ScriptContext.ENGINE_SCOPE).put("x", mapper.writeValueAsString(1.2f));
        assertEquals("1.2", jythonEngine.eval("str(graphson_io.readObject(x))"));
        assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x),float)"));
        //
        jythonEngine.getBindings(ScriptContext.ENGINE_SCOPE).put("x", mapper.writeValueAsString(1.3d));
        assertEquals("1.3", jythonEngine.eval("str(graphson_io.readObject(x))"));
        assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x),float)"));
    }

    @Test
    public void shouldDeserializeCollections() throws Exception {
        final Map<String, Number> map = new LinkedHashMap<>();
        map.put("a", 2);
        map.put("b", 2.3d);
        jythonEngine.getBindings(ScriptContext.ENGINE_SCOPE).put("x", mapper.writeValueAsString(map));
        assertEquals("{u'a': 2, u'b': 2.3}", jythonEngine.eval("str(graphson_io.readObject(x))"));
        assertEquals(2, jythonEngine.eval("graphson_io.readObject(x)['a']"));
        assertEquals(2.3d, jythonEngine.eval("graphson_io.readObject(x)['b']")); // jython is smart about double
        assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x)['a'],int)"));
        assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x)['b'],float)"));
        //
        final List<Object> list = Arrays.asList(g.V(1).next(), "hello", map, true);
        jythonEngine.getBindings(ScriptContext.ENGINE_SCOPE).put("x", mapper.writeValueAsString(list));
        assertEquals("[v[1], u'hello', {u'a': 2, u'b': 2.3}, True]", jythonEngine.eval("str(graphson_io.readObject(x))"));
        assertEquals(g.V(1).next().toString(), jythonEngine.eval("str(graphson_io.readObject(x)[0])"));
        assertEquals("hello", jythonEngine.eval("graphson_io.readObject(x)[1]"));
        assertEquals("{u'a': 2, u'b': 2.3}", jythonEngine.eval("str(graphson_io.readObject(x)[2])"));
        assertTrue((Boolean) jythonEngine.eval("graphson_io.readObject(x)[3]"));
        assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x)[0],Vertex)"));
        // assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x)[1],str)")); // its python unicode jython object
        assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x)[2],dict)"));
        assertTrue((Boolean) jythonEngine.eval("isinstance(graphson_io.readObject(x)[3],bool)"));
    }

}
