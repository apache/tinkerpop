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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Compare;
import org.apache.tinkerpop.gremlin.structure.P;
import org.junit.Test;

import java.util.Iterator;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ConjunctionStepTest {

    @Test
    public void shouldGetHasContainers() {
        final GraphTraversal.Admin<?, ?> traversal = and(has("name"), has("age", P.gt(30)).or(has("lang", "java"))).asAdmin();
        assertTrue(((ConjunctionStep) traversal.getStartStep()).isConjunctionHasTree());
        final ConjunctionStep.ConjunctionTree conjunctionTree = (((ConjunctionStep<?>) traversal.getStartStep()).getConjunctionHasTree());
        final Iterator<ConjunctionStep.ConjunctionTree.Entry> iterator = conjunctionTree.iterator();
        //System.out.println(conjunctionTree);
        ConjunctionStep.ConjunctionTree.Entry entry = iterator.next();
        assertTrue(entry.isHasContainer());
        assertEquals("name", entry.<HasContainer>getValue().key);
        //
        entry = iterator.next();
        assertTrue(entry.isHasContainer());
        assertEquals("age", entry.<HasContainer>getValue().key);
        assertEquals(Compare.gt, entry.<HasContainer>getValue().predicate);
        assertEquals(30, entry.<HasContainer>getValue().value);
        //
        entry = iterator.next();
        assertTrue(entry.isConjunctionTree());
        assertFalse(entry.<ConjunctionStep.ConjunctionTree>getValue().isAnd());
        entry = entry.<ConjunctionStep.ConjunctionTree>getValue().iterator().next();
        assertTrue(entry.isHasContainer());
        assertEquals("lang", entry.<HasContainer>getValue().key);
        assertEquals(Compare.eq, entry.<HasContainer>getValue().predicate);
        assertEquals("java", entry.<HasContainer>getValue().value);
    }

    @Test
    public void shouldNotGetHasContainers() {
        final GraphTraversal.Admin<?, ?> traversal = and(has("name"), has("age", P.gt(30)).or(has("lang", "java"), out().has("name", "josh"))).asAdmin();
        assertFalse(((ConjunctionStep) traversal.getStartStep()).isConjunctionHasTree());
    }
}
