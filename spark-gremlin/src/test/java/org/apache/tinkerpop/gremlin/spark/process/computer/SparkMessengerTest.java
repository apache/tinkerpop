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

package org.apache.tinkerpop.gremlin.spark.process.computer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

/**
 * @author Dean Zhu
 */
public class SparkMessengerTest extends AbstractSparkTest {
    private static ObjectMapper objectmapper = new ObjectMapper();

    @Test
    public void testSparkMessenger() throws Exception {
        // Define scopes
        final MessageScope.Local<String> orderSrcMessageScope = MessageScope.Local
                .of(__::inE, new BiFunction<String, Edge, String>() {
                    @Override
                    public String apply(String message, Edge edge) {
                        System.out.println(edge);
                        if ("mocked_edge_label1".equals(edge.label())) {
                            return message;
                        }
                        return null;
                    }
                });
        final MessageScope.Local<String> inMessageScope = MessageScope.Local.of(__::inE);

        // Define star graph
        final StarGraph starGraph = StarGraph.open();
        Object[] vertex0Array = new Object[]{T.id, 0, T.label, "mocked_vertex_label1"};
        Object[] vertex1Array = new Object[]{T.id, 1, T.label, "mocked_vertex_label2"};
        Object[] vertex2Array = new Object[]{T.id, 2, T.label, "mocked_vertex_label2"};
        Vertex vertex0 = starGraph.addVertex(vertex0Array);
        Vertex vertex1 = starGraph.addVertex(vertex1Array);
        Vertex vertex2 = starGraph.addVertex(vertex2Array);
        vertex1.addEdge("mocked_edge_label1", vertex0);
        vertex2.addEdge("mocked_edge_label2", vertex0);

        // Create Spark Messenger
        final SparkMessenger<String> messenger = new SparkMessenger<>();
        final List<String> incomingMessages = Arrays.asList("a", "b", "c");
        messenger.setVertexAndIncomingMessages(vertex0, incomingMessages);

        messenger.sendMessage(orderSrcMessageScope, "a");
        List<Tuple2<Object, String>> outgoingMessages0 = messenger.getOutgoingMessages();
        System.out.println(objectmapper.writeValueAsString(outgoingMessages0));

        Assert.assertEquals("a", outgoingMessages0.get(0)._2());
        Assert.assertNull(outgoingMessages0.get(1)._2());
        //messenger.sendMessage(inMessageScope, "a");
        //List<Tuple2<Object, String>> outgoingMessages1 = messenger.getOutgoingMessages();
        //System.out.println(objectmapper.writeValueAsString(outgoingMessages1));
    }
}