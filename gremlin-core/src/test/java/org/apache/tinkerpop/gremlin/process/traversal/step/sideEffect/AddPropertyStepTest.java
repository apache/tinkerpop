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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.GValueManager;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class AddPropertyStepTest extends GValueStepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.property("x", 0),
                __.property("x", 1),
               // __.property("y", 0)
                __.property("x", GValue.of("value", 0))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.property("x", GValue.of("value", 0)), Set.of("value"))
            );
    }

    @Test
    public void testGetPopInstructions() {
        final AddPropertyStep step = new AddPropertyStep(__.identity().select("s").asAdmin(), null, "x", 0);

        assertEquals(0, step.getPopInstructions().size());
    }

    @Test
    public void getValueShouldPinVariable() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property("x", GValue.of("value", 0)).asAdmin();
        assertNotNull(((AddPropertyStepPlaceholder) traversal.getSteps().get(0)).getValue());
        verifySinglePinnedVariable(traversal, "value");
    }

    @Test
    public void getValueGValueSafeShouldNotPinVariable() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property("x", GValue.of("value", 0)).asAdmin();
        assertNotNull(((AddPropertyStepPlaceholder) traversal.getSteps().get(0)).getValueGValueSafe());
        verifySingleUnpinnedVariable(traversal, "value");
    }

    @Test
    public void getPropertiesGValueSafeShouldNotPinVariable() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property("x", 0, "meta", GValue.of("metaValue", 1)).asAdmin();
        assertNotNull(((AddPropertyStepPlaceholder) traversal.getSteps().get(0)).getPropertiesGValueSafe());
        verifySingleUnpinnedVariable(traversal, "metaValue");
    }

    @Test
    public void getPropertiesShouldPinVariable() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property("x", 0, "meta", GValue.of("metaValue", 1)).asAdmin();
        assertNotNull(((AddPropertyStepPlaceholder) traversal.getSteps().get(0)).getProperties());
        verifySinglePinnedVariable(traversal, "metaValue");
    }

    private void verifySingleUnpinnedVariable(GraphTraversal.Admin<Object, Object> traversal, String value) {
        GValueManager gValueManager = traversal.getGValueManager();
        assertTrue(gValueManager.hasUnpinnedVariables());
        assertEquals(1, gValueManager.getUnpinnedVariableNames().size());
        assertEquals(value, gValueManager.getUnpinnedVariableNames().iterator().next());
    }

    private void verifySinglePinnedVariable(GraphTraversal.Admin<Object, Object> traversal, String variableName) {
        GValueManager gValueManager = traversal.getGValueManager();
        assertFalse(gValueManager.hasUnpinnedVariables());
        assertEquals(1, gValueManager.getPinnedVariableNames().size());
        assertEquals(variableName, gValueManager.getPinnedVariableNames().iterator().next());
    }
}
