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

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class DedupGlobalStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.dedup(),
                __.dedup().by("name")
        );
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowForMultipleByModulators() {
        __.dedup().by("name").by("age");
    }

    @Test
    public void testScopingInfo() {
        final DedupGlobalStep dedupGlobalStep = new DedupGlobalStep(__.identity().asAdmin(), "label1", "label2", "label1");

        final Scoping.ScopingInfo scopingInfo1 = new Scoping.ScopingInfo();
        scopingInfo1.label = "label1";
        scopingInfo1.pop = Pop.last;

        final Scoping.ScopingInfo scopingInfo2 = new Scoping.ScopingInfo();
        scopingInfo2.label = "label2";
        scopingInfo2.pop = Pop.last;

        final HashSet<Object> scopingInfoSet = new HashSet<>();
        scopingInfoSet.add(scopingInfo1);
        scopingInfoSet.add(scopingInfo2);

        assertEquals(dedupGlobalStep.getScopingInfo(), scopingInfoSet);
    }
}
