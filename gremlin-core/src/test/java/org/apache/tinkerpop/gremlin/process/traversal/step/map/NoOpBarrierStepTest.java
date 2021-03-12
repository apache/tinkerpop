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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Florian Grieskamp
 */
public class NoOpBarrierStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.barrier());
    }

    @Test
    public void testGetDefaultMaxBarrierSize() {
        final Traversal.Admin<?, ?> traversal = __.barrier().asAdmin();
        final NoOpBarrierStep<?> barrier = (NoOpBarrierStep<?>) traversal.getStartStep();
        assertEquals(Integer.MAX_VALUE, barrier.getMaxBarrierSize());
    }

    @Test
    public void testGetCustomMaxBarrierSize() {
        int customBarrierSize = 1234;
        final Traversal.Admin<?, ?> traversal = __.barrier(customBarrierSize).asAdmin();
        final NoOpBarrierStep<?> barrier = (NoOpBarrierStep<?>) traversal.getStartStep();
        assertEquals(customBarrierSize, barrier.getMaxBarrierSize());
    }
}
