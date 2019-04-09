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
package org.apache.tinkerpop.machine;

import org.apache.tinkerpop.language.gremlin.Gremlin;
import org.apache.tinkerpop.language.gremlin.Traversal;
import org.apache.tinkerpop.language.gremlin.TraversalSource;
import org.apache.tinkerpop.language.gremlin.TraversalUtil;
import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractTestSuite<C> {

    protected Machine machine;
    TraversalSource<C> g;

    AbstractTestSuite(final Machine machine, final Bytecode<C> source) {
        this.machine = machine;
        this.g = Gremlin.traversal(machine);
        BytecodeUtil.mergeSourceInstructions(source, TraversalUtil.getBytecode(this.g));
    }

    @AfterAll
    public void shutdown() {
        this.g.close();
        this.machine.close();
    }

    static <C, S, E> void verify(final List<E> expected, final Traversal<C, S, E> traversal) {
        final List<E> actual = traversal.toList();
        assertEquals(expected.size(), actual.size(), "Result set size differ: " + expected + " -- " + actual);
        for (final E e : expected) {
            assertTrue(actual.contains(e), "Result does not contain " + e + " -- " + actual);
        }
        assertFalse(traversal.hasNext());
    }

    static <C, S, E> void verifyOrder(final List<E> expected, final Traversal<C, S, E> traversal) {
        final List<E> actual = traversal.toList();
        assertEquals(expected.size(), actual.size(), "Result set size differ: " + expected + " -- " + actual);
        assertEquals(expected, actual, "Result sets are not order equal: " + expected + " -- " + actual);
        assertFalse(traversal.hasNext());
    }

    public static void sleep(final int time) {
        try {
            Thread.sleep(time);
        } catch (final InterruptedException e) {
            // do nothing
        }
    }

}
