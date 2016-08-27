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

package org.apache.tinkerpop.gremlin.python.driver;

import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalSideEffects;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@org.junit.Ignore
public class DriverRemoteConnectionTest {

    private static boolean PYTHON_EXISTS = false;

    @BeforeClass
    public static void setup() {
        try {
            final Optional<String> pythonVersion = new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec("python --version").getErrorStream()))
                    .lines()
                    .filter(line -> line.trim().startsWith("Python "))
                    .findAny();
            PYTHON_EXISTS = pythonVersion.isPresent();
            System.out.println("Python virtual machine: " + pythonVersion.orElse("None"));
            if (PYTHON_EXISTS)
                new GremlinServer(Settings.read(DriverRemoteConnectionTest.class.getResourceAsStream("gremlin-server-modern-secure-py.yaml"))).start().join();
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
    }

    private static List<String> submit(final String... scriptLines) throws IOException {
        final StringBuilder builder = new StringBuilder();
        builder.append("from gremlin_python import statics\n");
        builder.append("from gremlin_python.structure.graph import Graph\n");
        builder.append("from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection\n");
        builder.append("from gremlin_python.structure.io.graphson import GraphSONWriter\n\n");
        builder.append("statics.load_statics(globals())\n");
        builder.append("graph = Graph()\n");
        builder.append("g = graph.traversal().withRemote(DriverRemoteConnection('ws://localhost:8182','g',username='stephen', password='password'))\n");
        for (int i = 0; i < scriptLines.length - 1; i++) {
            builder.append(scriptLines[i] + "\n");
        }
        builder.append("final = " + scriptLines[scriptLines.length - 1] + "\n");
        builder.append("if isinstance(final,dict):\n");
        builder.append("  for key in final.keys():\n");
        builder.append("    print (str(key),str(final[key]))\n");
        builder.append("elif isinstance(final,str):\n");
        builder.append("  print final\n");
        builder.append("else:\n");
        builder.append("  for result in final:\n");
        builder.append("    print result\n\n");

        File file = TestHelper.generateTempFile(DriverRemoteConnectionTest.class, "temp", "py");
        final Writer writer = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
        writer.write(builder.toString());
        writer.flush();
        writer.close();

        final BufferedReader reader = new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec("python " + file.getAbsolutePath()).getInputStream()));
        final List<String> lines = reader.lines().map(String::trim).collect(Collectors.toList());
        reader.close();
        file.delete();
        return lines;

    }

    @Test
    public void testTraversals() throws Exception {
        if (!PYTHON_EXISTS) return;

        List<String> result = DriverRemoteConnectionTest.submit("g.V().count()");
        assertEquals(1, result.size());
        assertEquals("6", result.get(0));
        //
        result = DriverRemoteConnectionTest.submit("g.V(1).out('created').name");
        assertEquals(1, result.size());
        assertEquals("lop", result.get(0));
        //
        result = DriverRemoteConnectionTest.submit("g.V(1).out()");
        assertEquals(3, result.size());
        assertTrue(result.contains("v[4]"));
        assertTrue(result.contains("v[2]"));
        assertTrue(result.contains("v[3]"));
        //
        result = DriverRemoteConnectionTest.submit("g.V().repeat(out()).times(2).name");
        assertEquals(2, result.size());
        assertTrue(result.contains("lop"));
        assertTrue(result.contains("ripple"));
    }

    @Test
    public void testSideEffects() throws Exception {
        if (!PYTHON_EXISTS) return;

        List<String> result = DriverRemoteConnectionTest.submit(
                "t = g.V().out().iterate()",
                "str(t.side_effects)");
        assertEquals(1, result.size());
        assertEquals(new DefaultTraversalSideEffects().toString(), result.get(0));
        //
        result = DriverRemoteConnectionTest.submit(
                "t = g.V().out('created').groupCount('m').by('name').iterate()",
                "t.side_effects['m']");
        assertEquals(2, result.size());
        assertTrue(result.contains("('ripple', '1')"));
        assertTrue(result.contains("('lop', '3')"));
        //
        result = DriverRemoteConnectionTest.submit(
                "t = g.V().out('created').groupCount('m').by('name').aggregate('n').iterate()",
                "t.side_effects.keys()");
        assertEquals(2, result.size());
        assertTrue(result.contains("m"));
        assertTrue(result.contains("n"));
    }

}
