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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter

import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.Scope
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.junit.Test

import static __.bothE
import static __.sample

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySampleTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends SampleTest {

        @Override
        public Traversal<Edge, Edge> get_g_E_sampleX1X() {
            g.E.sample(1)
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_sampleX2X_byXweightX() {
            g.E.sample(2).by('weight')
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_localXoutE_sampleX1X_byXweightXX() {
            g.V.local(__.outE.sample(1).by('weight'))
        }

        @Override
        Traversal<Vertex, Map<String, Collection<Double>>> get_g_V_group_byXlabelX_byXbothE_valuesXweightX_foldX_byXsampleXlocal_2XX() {
            g.V().group().by(T.label).by(bothE().values('weight').fold()).by(sample(Scope.local, 2))
        }

        @Override
        Traversal<Vertex, Map<String, Collection<Double>>> get_g_V_group_byXlabelX_byXbothE_valuesXweightX_foldX_byXsampleXlocal_5XX() {
            g.V().group().by(T.label).by(bothE().values('weight').fold()).by(sample(Scope.local, 5))
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends SampleTest {

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_E_sampleX1X() {
            // TODO: makes no sense when its global
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_E_sampleX2X_byXweightX() {
            // TODO: makes no sense when its global
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_localXoutE_sampleX1X_byXweightXX() {
            ComputerTestHelper.compute("g.V.local(__.outE.sample(1).by('weight'))", g)
        }

        @Override
        Traversal<Vertex, Map<String, Collection<Double>>> get_g_V_group_byXlabelX_byXbothE_valuesXweightX_foldX_byXsampleXlocal_2XX() {
            ComputerTestHelper.compute("g.V().group().by(T.label).by(bothE().values('weight').fold()).by(sample(Scope.local, 2))", g)
        }

        @Override
        Traversal<Vertex, Map<String, Collection<Double>>> get_g_V_group_byXlabelX_byXbothE_valuesXweightX_foldX_byXsampleXlocal_5XX() {
            ComputerTestHelper.compute("g.V().group().by(T.label).by(bothE().values('weight').fold()).by(sample(Scope.local, 5))", g)
        }

        @Override
        Traversal<Edge, Edge> get_g_E_sampleX1X() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        Traversal<Edge, Edge> get_g_E_sampleX2X_byXweightX() {
            // override with nothing until the test itself is supported
            return null
        }
    }
}
