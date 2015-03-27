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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class ReadOnlyStrategyTest {
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"addInE()", __.addInE("test", "x")},
                {"addInE(args)", __.addInE("test", "x", "this", "that")},
                {"addOutE()", __.addOutE("test", "x")},
                {"addOutE(args)", __.addOutE("test", "x", "this", "that")},
                {"addE(IN)", __.addE(Direction.IN, "test", "test")},
                {"addE(IN,args)", __.addE(Direction.IN, "test", "test", "this", "that")},
                {"addE(OUT)", __.addE(Direction.OUT, "test", "test")},
                {"addE(OUT,args)", __.addE(Direction.OUT, "test", "test", "this", "that")},
                {"outE().property(k,v)", __.outE().property("test", "test")},
                {"out().properties(k).property(k,v)", __.out().properties("test").property("test", "that")},
                {"out().property(k,v)", __.out().property("test", "test")},
                {"out().property(Cardinality,k,v)", __.out().property(VertexProperty.Cardinality.list, "test", "test")},
                {"addV(args)", __.addV("test", "test", "this", "that")},
                {"addV()", __.addV()}});
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Traversal traversal;

    @Test
    public void shouldPreventMutatingStepsFromBeingInTheTraversal() {
        try {
            ReadOnlyStrategy.instance().apply(traversal.asAdmin());
            fail("The strategy should have found a mutating step.");
        } catch (IllegalStateException ise) {
            assertEquals("The provided traversal has a mutating step and thus is not read only", ise.getMessage());
        }
    }
}
