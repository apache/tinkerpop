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
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DriverRemoteConnectionTest {

    private static boolean PYTHON_EXISTS = false;

    @BeforeClass
    public static void setup() {
        try {
            PYTHON_EXISTS = new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec("python --version").getErrorStream())).lines().filter(line -> line.trim().startsWith("Python ")).findAny().isPresent();
            System.out.println("Python exists: " + PYTHON_EXISTS);
            if (PYTHON_EXISTS)
                new GremlinServer(Settings.read(DriverRemoteConnectionTest.class.getResourceAsStream("gremlin-server-modern-py.yaml"))).start().join();
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
    }

    private static List<String> submit(final String traversal) throws IOException {
        final StringBuilder script = new StringBuilder();
        script.append("from gremlin_python import statics\n");
        script.append("from gremlin_python.structure.graph import Graph\n");
        script.append("from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection\n");
        script.append("from gremlin_python.structure.io.graphson import GraphSONWriter\n\n");
        script.append("statics.load_statics(globals())\n");
        script.append("graph = Graph()\n");
        script.append("g = graph.traversal().withRemote(DriverRemoteConnection('ws://localhost:8182','g',username='stephen', password='password'))\n");
        script.append("results = " + traversal + ".toList()\n");
        script.append("print results\n\n");

        File file = TestHelper.generateTempFile(DriverRemoteConnectionTest.class, "temp", "py");
        final Writer writer = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
        writer.write(script.toString());
        writer.flush();
        writer.close();

        final BufferedReader reader = new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec("python " + file.getAbsolutePath()).getInputStream()));
        final List<String> lines = reader.lines()
                .map(line -> line.substring(1, line.length() - 1))
                .flatMap(line -> Arrays.stream(line.split(",")))
                .map(String::trim)
                .collect(Collectors.toList());
        reader.close();
        file.delete();
        return lines;

    }

    @Test
    public void testTraversals() throws Exception {
        if (!PYTHON_EXISTS) return;

        List<String> result = DriverRemoteConnectionTest.submit("g.V().count()");
        assertEquals(1, result.size());
        assertEquals("6L", result.get(0));
        //
        result = DriverRemoteConnectionTest.submit("g.V(1).out('created').name");
        assertEquals(1, result.size());
        assertEquals("u'lop'", result.get(0));
        //
        result = DriverRemoteConnectionTest.submit("g.V(1).out()");
        assertEquals(3, result.size());
        assertTrue(result.contains("v[4]"));
        assertTrue(result.contains("v[2]"));
        assertTrue(result.contains("v[3]"));
    }

    @Test
    public void testAnonymousTraversals() throws Exception {
        if (!PYTHON_EXISTS) return;

        List<String> result = DriverRemoteConnectionTest.submit("g.V().repeat(out()).times(2).name");
        assertEquals(2, result.size());
        assertTrue(result.contains("u'lop'"));
        assertTrue(result.contains("u'ripple'"));
    }

}
