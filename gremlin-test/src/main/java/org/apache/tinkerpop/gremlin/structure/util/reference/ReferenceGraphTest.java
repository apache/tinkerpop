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
package org.apache.tinkerpop.gremlin.structure.util.reference;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceGraphTest extends AbstractGremlinTest {

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void testAttachableGetMethod() {
        // vertex host
        g.V().forEachRemaining(vertex -> TestHelper.validateEquality(vertex, ReferenceFactory.detach(vertex).attach(Attachable.Method.get(vertex))));
        g.V().forEachRemaining(vertex -> vertex.properties().forEachRemaining(vertexProperty -> TestHelper.validateEquality(vertexProperty, ReferenceFactory.detach(vertexProperty).attach(Attachable.Method.get(vertex)))));
        g.V().forEachRemaining(vertex -> vertex.properties().forEachRemaining(vertexProperty -> vertexProperty.properties().forEachRemaining(property -> TestHelper.validateEquality(property, ReferenceFactory.detach(property).attach(Attachable.Method.get(vertex))))));
        g.V().forEachRemaining(vertex -> vertex.edges(Direction.OUT).forEachRemaining(edge -> TestHelper.validateEquality(edge, ReferenceFactory.detach(edge).attach(Attachable.Method.get(vertex)))));
        g.V().forEachRemaining(vertex -> vertex.edges(Direction.OUT).forEachRemaining(edge -> edge.properties().forEachRemaining(property -> TestHelper.validateEquality(property, ReferenceFactory.detach(property).attach(Attachable.Method.get(vertex))))));

        // graph host
        g.V().forEachRemaining(vertex -> TestHelper.validateEquality(vertex, ReferenceFactory.detach(vertex).attach(Attachable.Method.get(graph))));
        g.V().forEachRemaining(vertex -> vertex.properties().forEachRemaining(vertexProperty -> TestHelper.validateEquality(vertexProperty, ReferenceFactory.detach(vertexProperty).attach(Attachable.Method.get(graph)))));
        g.V().forEachRemaining(vertex -> vertex.properties().forEachRemaining(vertexProperty -> vertexProperty.properties().forEachRemaining(property -> TestHelper.validateEquality(property, ReferenceFactory.detach(property).attach(Attachable.Method.get(graph))))));
        g.V().forEachRemaining(vertex -> vertex.edges(Direction.OUT).forEachRemaining(edge -> TestHelper.validateEquality(edge, ReferenceFactory.detach(edge).attach(Attachable.Method.get(graph)))));
        g.V().forEachRemaining(vertex -> vertex.edges(Direction.OUT).forEachRemaining(edge -> edge.properties().forEachRemaining(property -> TestHelper.validateEquality(property, ReferenceFactory.detach(property).attach(Attachable.Method.get(graph))))));

    }
}