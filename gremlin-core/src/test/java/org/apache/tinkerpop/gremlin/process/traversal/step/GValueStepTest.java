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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class GValueStepTest extends StepTest {

    /**
     * Return a list of traversals for which one or more GValues are passed to the step to be tested. None of the
     * traversals will be executed during the tests, hence the traversal may be invalid. It's only important to provide
     * as many distinct scenarios for the step as possible.
     *
     * @return List of test pairs where LHS is a Traversal and the RHS is the expected list of variables to be tracked.
     */
    protected abstract List<Pair<Traversal, Set<String>>> getGValueTraversals();

    @Test
    public void testGValuesAreTracked() {
        for (Pair<Traversal, Set<String>> gValueTraversal : getGValueTraversals()) {
            assertEquals(gValueTraversal.getRight(), gValueTraversal.getLeft().asAdmin().getGValueManager().getVariableNames());
        }
    }
}
