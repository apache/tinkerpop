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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by Benjamin Han
 */
public class GraphSONReaderTest extends AbstractGremlinTest {

    @Test
    public void shouldReadWriteSelfLoopingEdges() {
        final Configuration sourceConf = graphProvider.newGraphConfiguration("source", this.getClass(), name.getMethodName(), null);
        final Graph source = GraphFactory.open(sourceConf);
        Vertex v1 = source.addVertex();
        Vertex v2 = source.addVertex();
        v1.addEdge("CONTROL", v2);
        v1.addEdge("SELF-LOOP", v1);

        final Configuration targetConf = graphProvider.newGraphConfiguration("target", this.getClass(), name.getMethodName(), null);
        final Graph target = GraphFactory.open(targetConf);
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            source.io(IoCore.graphson()).writer().create().writeGraph(os, source);
            ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
            target.io(IoCore.graphson()).reader().create().readGraph(is, target);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        assertEquals(source.traversal().V().count(), target.traversal().V().count());
        assertEquals(source.traversal().E().count(), target.traversal().E().count());
    }
}
