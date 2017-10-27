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
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.python.jsr223.JythonScriptEngineSetup;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Ignore;
import org.junit.Test;
import org.python.jsr223.PyScriptEngine;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@Ignore
public class GraphSONWriterTest {

    private static final ScriptEngine jythonEngine = JythonScriptEngineSetup.setup((PyScriptEngine) new ScriptEngineManager().getEngineByName("jython"));
    private static final ObjectMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V2_0).create().createMapper();

    @Test
    public void shouldSerializeNumbers() throws Exception {
        assertEquals(1, mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(1)").toString(), Object.class));
        assertEquals(mapper.writeValueAsString(1), jythonEngine.eval("graphson_writer.writeObject(1)").toString().replace(" ", ""));
        //
        assertEquals(2L, mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(2L)").toString(), Object.class));
        assertEquals(mapper.writeValueAsString(2L), jythonEngine.eval("graphson_writer.writeObject(2L)").toString().replace(" ", ""));
        //
        assertEquals(3.4, mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(3.4)").toString(), Object.class));
        assertEquals(mapper.writeValueAsString(3.4), jythonEngine.eval("graphson_writer.writeObject(3.4)").toString().replace(" ", ""));
    }

    @Test
    public void shouldSerializeCollections() throws Exception {
        final Map<String, Number> map = new LinkedHashMap<>();
        map.put("a", 2);
        map.put("b", 2.3);
        assertEquals(map, mapper.readValue(jythonEngine.eval("graphson_writer.writeObject({'a':2,'b':2.3})").toString(), Object.class));
        //
        final List<Object> list = Arrays.asList(new DefaultRemoteTraverser<>("hello", 3L), "hello", map, true);
        assertTrue((Boolean) jythonEngine.eval("isinstance([Traverser('hello',3L),'hello',{'a':2,'b':2.3},True],list)"));
        assertEquals(list, mapper.readValue(jythonEngine.eval("graphson_writer.writeObject([Traverser('hello',3L),'hello',{'a':2,'b':2.3},True])").toString(), Object.class));
    }

    @Test
    public void shouldSerializeTraverser() throws Exception {
        assertEquals(
                new DefaultRemoteTraverser<>("hello", 3L),
                mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(Traverser('hello',3L))").toString(), Object.class));
        assertEquals(3L, mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(Traverser('hello',3L))").toString(), Traverser.class).bulk());
        assertEquals("hello", mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(Traverser('hello',3L))").toString(), Traverser.class).get());
    }

    @Test
    public void shouldSerializeBytecode() throws Exception {
        assertEquals(P.eq(7L), mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(P.eq(7L))").toString(), Object.class));
        // TODO: assertEquals(mapper.writeValueAsString(P.between(1, 2).and(P.eq(7L))), jythonEngine.eval("graphson_writer.writeObject(P.eq(7L)._and(P.between(1,2)))").toString().replace(" ",""));
        assertEquals(AndP.class, mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(P.eq(7L).and_(P.between(1,2)))").toString(), Object.class).getClass());
        //
        assertEquals(new Bytecode.Binding<>("a", 5L), mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(Binding('a',5L))").toString(), Object.class));
        //
        for (final Column t : Column.values()) {
            assertEquals(t, mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(Column." + t.name() + ")").toString(), Object.class));
        }
        for (final T t : T.values()) {
            assertEquals(t, mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(T." + t.name() + ")").toString(), Object.class));
        }
        assertEquals(Pop.first, mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(Pop.first)").toString(), Object.class));
        assertEquals(Pop.last, mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(Pop.last)").toString(), Object.class));
        assertEquals(Pop.all, mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(Pop.all_)").toString(), Object.class));
        assertEquals(Scope.global, mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(Scope.global_)").toString(), Object.class));
        assertEquals(Scope.local, mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(Scope.local)").toString(), Object.class));
    }

    @Test
    public void shouldSerializeLambda() throws Exception {
        assertEquals(
                Lambda.function("lambda z : 1+2", "gremlin-python"),
                mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(lambda : 'z : 1+2')").toString(), Object.class));
        assertEquals(
                Lambda.function("lambda z : z+ 7", "gremlin-python"),
                mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(lambda : 'lambda z : z+ 7')").toString(), Object.class));
        assertEquals(
                Lambda.supplier("lambda : 23", "gremlin-python"),
                mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(lambda : 'lambda : 23')").toString(), Object.class));
        assertEquals(
                Lambda.consumer("lambda z : z + 23", "gremlin-python"),
                mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(lambda : 'lambda z : z + 23')").toString(), Object.class));
        assertEquals(
                Lambda.biFunction("lambda z,y : z - y + 2", "gremlin-python"),
                mapper.readValue(jythonEngine.eval("graphson_writer.writeObject(lambda : 'lambda z,y : z - y + 2')").toString(), Object.class));
    }

}
