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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BytecodeUtilTest {
    private static final GraphTraversalSource g = EmptyGraph.instance().traversal();
    
    @Test
    public void shouldFindStrategy() {
        final Iterator<OptionsStrategy> itty = BytecodeUtil.findStrategies(g.with("x").with("y", 100).V().asAdmin().getBytecode(), OptionsStrategy.class);
        int counter = 0;
        while(itty.hasNext()) {
            final OptionsStrategy strategy = itty.next();
            if (strategy.getOptions().keySet().contains("x")) {
                assertThat(strategy.getOptions().get("x"), is(true));
                counter++;
            } else if (strategy.getOptions().keySet().contains("y")) {
                assertEquals(100, strategy.getOptions().get("y"));
                counter++;
            }
        }

        assertEquals(2, counter);
    }

    @Test
    public void shouldNotFindStrategy() {
        final Iterator<ReadOnlyStrategy> itty = BytecodeUtil.findStrategies(g.with("x").with("y", 100).V().asAdmin().getBytecode(), ReadOnlyStrategy.class);
        assertThat(itty.hasNext(), is(false));
    }
}
