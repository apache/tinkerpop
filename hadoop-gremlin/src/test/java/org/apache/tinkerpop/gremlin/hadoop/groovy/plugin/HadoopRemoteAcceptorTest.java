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

package org.apache.tinkerpop.gremlin.hadoop.groovy.plugin;

import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

public class HadoopRemoteAcceptorTest extends AbstractGremlinProcessTest {

    private boolean ignore = false;

    @Before
    public void setup() throws Exception {
        if (GraphManager.getGraphProvider() != null) {
            super.setup();
        } else {
            this.ignore = true;
        }
    }

    @Before
    public void setupTest() {
        if (GraphManager.getGraphProvider() != null) {
            super.setupTest();
        } else {
            this.ignore = true;
        }
    }

    ///////////////////

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldReturnResultIterator() throws Exception {
        if (!ignore) {
            final HadoopRemoteAcceptor remoteAcceptor = new HadoopRemoteAcceptor(new Groovysh());
            remoteAcceptor.hadoopGraph = (HadoopGraph) graph;
            Traversal<?, ?> traversal = (Traversal<?, ?>) remoteAcceptor.submit(Arrays.asList("g.V().count()"));
            assertEquals(6L, traversal.next());
            assertFalse(traversal.hasNext());
        }
    }

}
