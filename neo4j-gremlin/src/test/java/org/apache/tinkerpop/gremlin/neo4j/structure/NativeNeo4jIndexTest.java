/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.neo4j.structure;

import org.apache.tinkerpop.gremlin.neo4j.AbstractNeo4jGremlinTest;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class NativeNeo4jIndexTest extends AbstractNeo4jGremlinTest {

    @Test
    public void shouldHaveFasterRuntimeWithLabelKeyValueIndex() throws Exception {
        final Neo4jGraph neo4j = (Neo4jGraph) this.graph;
        for (int i = 0; i < 10000; i++) {
            if (i % 2 == 0)
                this.graph.addVertex(T.label, "something", "myId", i);
            else
                this.graph.addVertex(T.label, "nothing", "myId", i);
        }
        this.graph.tx().commit();
        final Runnable traversal = () -> g.V().hasLabel("something").has("myId", 2000).tryNext().get();

        // no index
        TimeUtil.clock(10, traversal);
        final double noIndexTime = TimeUtil.clock(20, traversal);
        // index time
        neo4j.cypher("CREATE INDEX ON :something(myId)").iterate();
        this.graph.tx().commit();
        Thread.sleep(5000); // wait for index to be build just in case
        TimeUtil.clock(10, traversal);
        final double indexTime = TimeUtil.clock(20, traversal);
        //System.out.println(noIndexTime + "----" + indexTime);
        assertTrue(noIndexTime > indexTime);
    }
}
