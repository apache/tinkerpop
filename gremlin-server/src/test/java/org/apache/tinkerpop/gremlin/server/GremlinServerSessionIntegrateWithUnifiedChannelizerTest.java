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
package org.apache.tinkerpop.gremlin.server;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.channel.UnifiedChannelizerIntegrateTest;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GremlinServerSessionIntegrateWithUnifiedChannelizerTest extends UnifiedChannelizerIntegrateTest {

    /**
     * Reproducer for TINKERPOP-2751
     */
    @Test(timeout=30000)
    public void shouldThrowExceptionOnTransactionUnsupportedGraph() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        final GraphTraversalSource gtx = g.tx().begin();
        assertThat(gtx.tx().isOpen(), is(true));

        gtx.addV("person").iterate();
        assertEquals(1, (long) gtx.V().count().next());

        try {
            // Without Neo4j plugin this should fail on gremlin-server
            gtx.tx().commit();
            fail("commit should throw exception on non-transaction supported graph");
        } catch (Exception ex){
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertEquals("Graph does not support transactions", root.getMessage());
        }

        cluster.close();
    }
}
