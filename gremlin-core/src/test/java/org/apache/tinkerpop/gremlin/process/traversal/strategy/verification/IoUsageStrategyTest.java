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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class IoUsageStrategyTest {

    private static final GraphTraversalSource g = EmptyGraph.instance().traversal();

    private static File junkFile;

    static {
        try {
            junkFile = TestHelper.generateTempFile(IoUsageStrategyTest.class, "fake", "kryo");
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"g.read('a.kryo')", g.read(junkFile.getAbsolutePath()), true},
                {"g.write('a.kryo')", g.write(junkFile.getAbsolutePath()), true},
                {"g.read('a.kryo').with(\"x\", \"y\")", g.read(junkFile.getAbsolutePath()).with("x", "y"), true},
                {"g.write('a.kryo').with(\"x\", \"y\")", g.write(junkFile.getAbsolutePath()).with("x", "y"), true},
                {"g.read('a.kryo').V()", g.read(junkFile.getAbsolutePath()).V(), false},
                {"g.write('a.kryo').V()", g.write(junkFile.getAbsolutePath()).V(), false}
        });
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Traversal traversal;

    @Parameterized.Parameter(value = 2)
    public boolean allow;

    @Test
    public void shouldBeVerified() {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(IoUsageStrategy.instance());
        traversal.asAdmin().setStrategies(strategies);
        if (allow) {
            traversal.asAdmin().applyStrategies();
        } else {
            try {
                traversal.asAdmin().applyStrategies();
                fail("The strategy should not allow read()/write() to be used with other steps: " + this.traversal);
            } catch (VerificationException ise) {
                assertTrue(ise.getMessage().contains("read() or write() steps"));
            }
        }
    }
}
