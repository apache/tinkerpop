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

package org.apache.tinkerpop.gremlin.akka.process.actors

import org.apache.tinkerpop.gremlin.process.computer.Computer
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
import org.apache.tinkerpop.gremlin.util.TimeUtil
import org.junit.Ignore
import org.junit.Test

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class AkkaGroovyPlayTest {

    @Test
    @Ignore
    public void testStuff() {

        def graph = TinkerGraph.open()
        def g = graph.traversal()
        def a = graph.traversal().withProcessor(AkkaGraphActors.open().workers(8));
        def r = new Random(123)

        (1..1000000).each {
            def vid = ["a", "b", "c", "d"].collectEntries { [it, r.nextInt() % 400000] }
            graph.addVertex(T.id, vid)
        }; []

        println TimeUtil.clockWithResult(1) { g.V().id().select("c").count().next() }
        println TimeUtil.clockWithResult(1) { g.V().id().select("c").dedup().count().next() }
        println TimeUtil.clockWithResult(1) { a.V().id().select("c").count().next() }
        println TimeUtil.clockWithResult(1) { a.V().id().select("c").dedup().count().next() }
    }
}
