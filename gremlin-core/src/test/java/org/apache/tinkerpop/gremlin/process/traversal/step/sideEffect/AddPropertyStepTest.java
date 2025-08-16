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

    private static final String PROPERTY_NAME = "x";
    private static final int PROPERTY_VALUE = 0;
    private static final String GVALUE_NAME = "value";
    private static final String META_PROPERTY_NAME = "meta";
    private static final String GVALUE_META_NAME = "metaValue";
    private static final int META_PROPERTY_VALUE = 1;

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.property(PROPERTY_NAME, PROPERTY_VALUE),
                __.property(PROPERTY_NAME, META_PROPERTY_VALUE),
               // __.property("y", 0)
                __.property(PROPERTY_NAME, GValue.of(GVALUE_NAME, PROPERTY_VALUE))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.property(PROPERTY_NAME, GValue.of(GVALUE_NAME, PROPERTY_VALUE)), Set.of(GVALUE_NAME))
            );
    }

    @Test
    public void testGetPopInstructions() {
        final AddPropertyStep step = new AddPropertyStep(__.identity().select("s").asAdmin(), null, PROPERTY_NAME, PROPERTY_VALUE);

        assertEquals(PROPERTY_VALUE, step.getPopInstructions().size());
    }
    
    @Test
    public void getKeyFromConcreteStep() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PROPERTY_NAME, PROPERTY_VALUE).asAdmin();
        assertEquals(PROPERTY_NAME, ((AddPropertyStepPlaceholder) traversal.getSteps().get(PROPERTY_VALUE)).asConcreteStep().getKey());
    }

    @Test
    public void getValueFromConcreteStep() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PROPERTY_NAME, PROPERTY_VALUE).asAdmin();
        assertEquals(PROPERTY_VALUE, ((AddPropertyStepPlaceholder) traversal.getSteps().get(PROPERTY_VALUE)).asConcreteStep().getValue());
    }
    
    @Test
    public void getKeyShouldNotPinVariable() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PROPERTY_NAME, GValue.of(GVALUE_NAME, PROPERTY_VALUE)).asAdmin();
        assertEquals(PROPERTY_NAME, ((AddPropertyStepPlaceholder) traversal.getSteps().get(PROPERTY_VALUE)).getKey());
        verifySingleUnpinnedVariable(traversal, GVALUE_NAME);
    }

    @Test
    public void getValueShouldPinVariable() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PROPERTY_NAME, GValue.of(GVALUE_NAME, PROPERTY_VALUE)).asAdmin();
        assertNotNull(((AddPropertyStepPlaceholder) traversal.getSteps().get(PROPERTY_VALUE)).getValue());
        verifySinglePinnedVariable(traversal, GVALUE_NAME);
    }

    @Test
    public void getValueGValueSafeShouldNotPinVariable() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PROPERTY_NAME, GValue.of(GVALUE_NAME, PROPERTY_VALUE)).asAdmin();
        assertNotNull(((AddPropertyStepPlaceholder) traversal.getSteps().get(PROPERTY_VALUE)).getValueGValueSafe());
        verifySingleUnpinnedVariable(traversal, GVALUE_NAME);
    }

    @Test
    public void getPropertiesGValueSafeShouldNotPinVariable() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PROPERTY_NAME, PROPERTY_VALUE, META_PROPERTY_NAME, GValue.of(GVALUE_META_NAME, META_PROPERTY_VALUE)).asAdmin();
        assertNotNull(((AddPropertyStepPlaceholder) traversal.getSteps().get(PROPERTY_VALUE)).getPropertiesGValueSafe());
        verifySingleUnpinnedVariable(traversal, GVALUE_META_NAME);
    }

    @Test
    public void getPropertiesShouldPinVariable() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PROPERTY_NAME, PROPERTY_VALUE, META_PROPERTY_NAME, GValue.of(GVALUE_META_NAME, META_PROPERTY_VALUE)).asAdmin();
        assertNotNull(((AddPropertyStepPlaceholder) traversal.getSteps().get(PROPERTY_VALUE)).getProperties());
        verifySinglePinnedVariable(traversal, GVALUE_META_NAME);
    }

    private void verifySingleUnpinnedVariable(GraphTraversal.Admin<Object, Object> traversal, String value) {
        GValueManager gValueManager = traversal.getGValueManager();
        assertTrue(gValueManager.hasUnpinnedVariables());
        assertEquals(META_PROPERTY_VALUE, gValueManager.getUnpinnedVariableNames().size());
        assertEquals(value, gValueManager.getUnpinnedVariableNames().iterator().next());
    }

    private void verifySinglePinnedVariable(GraphTraversal.Admin<Object, Object> traversal, String variableName) {
        GValueManager gValueManager = traversal.getGValueManager();
        assertFalse(gValueManager.hasUnpinnedVariables());
        assertEquals(META_PROPERTY_VALUE, gValueManager.getPinnedVariableNames().size());
        assertEquals(variableName, gValueManager.getPinnedVariableNames().iterator().next());
    }
}
