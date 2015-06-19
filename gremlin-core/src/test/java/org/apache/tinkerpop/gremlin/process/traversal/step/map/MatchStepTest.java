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

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MatchStepTest extends StepTest {
    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.match("a", as("a").out("knows").as("b")),
                __.match(as("a").out("knows").as("b")),
                __.match("a", as("a").out().as("b")),
                __.match(as("a").out().as("b")),
                ////
                __.match("a", where(as("a").out("knows").as("b"))),
                __.match(where(as("a").out("knows").as("b"))),
                __.match("a", as("a").where(out().as("b"))),
                __.match(as("a").where(out().as("b")))
        );
    }

    @Test
    public void testCountMatchAlgorithm() {
        // MAKE SURE THE SORT ORDER CHANGES AS MORE RESULTS ARE RETURNED BY ONE OR THE OTHER TRAVERSAL
        Traversal.Admin<?, ?> traversal = __.match("a", as("a").out().as("b"), as("c").in().as("d")).asAdmin();
        MatchStep.CountMatchAlgorithm countMatchAlgorithm = new MatchStep.CountMatchAlgorithm();
        countMatchAlgorithm.initialize(((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren());
        assertEquals(2, countMatchAlgorithm.counts.size());
        countMatchAlgorithm.counts.stream().forEach(ints -> assertEquals(Integer.valueOf(0), ints[1]));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(0));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(0));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(1));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(1));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(1));
        assertEquals(Integer.valueOf(0), countMatchAlgorithm.counts.get(0)[0]);
        assertEquals(Integer.valueOf(2), countMatchAlgorithm.counts.get(0)[1]);
        //
        assertEquals(Integer.valueOf(1), countMatchAlgorithm.counts.get(1)[0]);
        assertEquals(Integer.valueOf(3), countMatchAlgorithm.counts.get(1)[1]);
        // CHECK RE-SORTING AS MORE DATA COMES IN
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(0));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(0));
        assertEquals(Integer.valueOf(1), countMatchAlgorithm.counts.get(0)[0]);
        assertEquals(Integer.valueOf(3), countMatchAlgorithm.counts.get(0)[1]);
        //
        assertEquals(Integer.valueOf(0), countMatchAlgorithm.counts.get(1)[0]);
        assertEquals(Integer.valueOf(4), countMatchAlgorithm.counts.get(1)[1]);


        ///////  MAKE SURE WHERE PREDICATE TRAVERSALS ARE ALWAYS FIRST AS THEY ARE SIMPLY .hasNext() CHECKS
        traversal = __.match("a", as("a").out().as("b"), as("c").in().as("d"), where("a", P.eq("b"))).asAdmin();
        countMatchAlgorithm = new MatchStep.CountMatchAlgorithm();
        countMatchAlgorithm.initialize(((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren());
        assertEquals(3, countMatchAlgorithm.counts.size());
        countMatchAlgorithm.counts.stream().forEach(ints -> assertEquals(Integer.valueOf(0), ints[1]));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(0));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(1));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(1));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(2));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(2));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(2));
        //
        assertEquals(Integer.valueOf(2), countMatchAlgorithm.counts.get(0)[0]);
        assertEquals(Integer.valueOf(3), countMatchAlgorithm.counts.get(0)[1]);
        //
        assertEquals(Integer.valueOf(0), countMatchAlgorithm.counts.get(1)[0]);
        assertEquals(Integer.valueOf(1), countMatchAlgorithm.counts.get(1)[1]);
        //
        assertEquals(Integer.valueOf(1), countMatchAlgorithm.counts.get(2)[0]);
        assertEquals(Integer.valueOf(2), countMatchAlgorithm.counts.get(2)[1]);
        //
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(2));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(2));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(2));
        //
        assertEquals(Integer.valueOf(2), countMatchAlgorithm.counts.get(0)[0]);
        assertEquals(Integer.valueOf(6), countMatchAlgorithm.counts.get(0)[1]);
        //
        assertEquals(Integer.valueOf(0), countMatchAlgorithm.counts.get(1)[0]);
        assertEquals(Integer.valueOf(1), countMatchAlgorithm.counts.get(1)[1]);
        //
        assertEquals(Integer.valueOf(1), countMatchAlgorithm.counts.get(2)[0]);
        assertEquals(Integer.valueOf(2), countMatchAlgorithm.counts.get(2)[1]);
        //

    }
}
