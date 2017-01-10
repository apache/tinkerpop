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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class StepTest {

    /**
     * Return a list of traversals for which the last step is the step to be tested. None of the traversals will be
     * executed during the tests, hence the traversal may be invalid. It's only important to provide as many distinct
     * scenarios for the last step as possible.
     *
     * @return List of traversals.
     */
    protected abstract List<Traversal> getTraversals();

    protected List<Step> getStepInstances() {
        return this.getTraversals().stream().map(t -> t.asAdmin().getEndStep()).collect(Collectors.toList());
    }

    @Test
    public void testEquality() {
        final List<Step> instances1 = this.getStepInstances();
        final List<Step> instances2 = this.getStepInstances();
        for (int i = 0; i < instances1.size(); i++) {
            final Step instance1 = instances1.get(i);
            assertEquals(instance1, instance1.clone());
            assertEquals(instance1.hashCode(), instance1.clone().hashCode());
            for (int j = 0; j < instances2.size(); j++) {
                final Step instance2 = instances2.get(j);
                if (i != j) {
                    assertNotEquals(instance1, instance2);
                } else {
                    assertEquals(instance1, instance2);
                    assertEquals(instance1.hashCode(), instance2.hashCode());
                }
            }
        }
    }
}
