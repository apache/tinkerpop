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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class AddPropertyStepTest extends GValueStepTest {

    private static final String PNAME = "x";
    private static final int PVALUE = 0;
    private static final String GNAME = "value";
    private static final String META_NAME = "meta";
    private static final String GMETA_NAME = "metaValue";
    private static final int META_VALUE = 1;

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.property(PNAME, PVALUE),
                __.property(PNAME, META_VALUE),
               // __.property("y", 0)
                __.property(PNAME, GValue.of(GNAME, PVALUE))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.property(PNAME, GValue.of(GNAME, PVALUE)), Set.of(GNAME))
            );
    }

    @Test
    public void testGetPopInstructions() {
        final AddPropertyStep step = new AddPropertyStep(__.identity().select("s").asAdmin(), null, PNAME, PVALUE);

        assertEquals(PVALUE, step.getPopInstructions().size());
    }
    
    @Test
    public void getKeyFromConcreteStep() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PNAME, PVALUE).asAdmin();
        assertEquals(PNAME, ((AddPropertyStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getKey());
    }

    @Test
    public void getValueFromConcreteStep() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PNAME, PVALUE).asAdmin();
        assertEquals(PVALUE, ((AddPropertyStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getValue());
    }
    
    @Test
    public void getKeyShouldNotPinVariable() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PNAME, GValue.of(GNAME, PVALUE)).asAdmin();
        assertEquals(PNAME, ((AddPropertyStepPlaceholder) traversal.getSteps().get(0)).getKey());
        verifySingleUnpinnedVariable(traversal, GNAME);
    }

    @Test
    public void getValueShouldPinVariable() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PNAME, GValue.of(GNAME, PVALUE)).asAdmin();
        assertEquals(PVALUE, ((AddPropertyStepPlaceholder) traversal.getSteps().get(0)).getValue());
        verifySinglePinnedVariable(traversal, GNAME);
    }

    @Test
    public void getValueAsGValueShouldNotPinVariable() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PNAME, GValue.of(GNAME, PVALUE)).asAdmin();
        assertEquals(GValue.of(GNAME, PVALUE), ((AddPropertyStepPlaceholder) traversal.getSteps().get(0)).getValueWithGValue());
        verifySingleUnpinnedVariable(traversal, GNAME);
    }

    @Test
    public void getPropertiesWithGValuesShouldNotPinVariable() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PNAME, PVALUE, META_NAME, GValue.of(GMETA_NAME, META_VALUE)).asAdmin();
        assertEquals(List.of(GValue.of(GMETA_NAME, META_VALUE)), ((AddPropertyStepPlaceholder) traversal.getSteps().get(0)).getPropertiesWithGValues().get(META_NAME));
        verifySingleUnpinnedVariable(traversal, GMETA_NAME);
    }

    @Test
    public void getPropertiesShouldPinVariable() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PNAME, PVALUE, META_NAME, GValue.of(GMETA_NAME, META_VALUE)).asAdmin();
        assertEquals(List.of(META_VALUE), ((AddPropertyStepPlaceholder) traversal.getSteps().get(0)).getProperties().get(META_NAME));
        verifySinglePinnedVariable(traversal, GMETA_NAME);
    }

    @Test
    public void getGValuesShouldReturnAllGValues() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PNAME, GValue.of(GNAME, PVALUE), META_NAME, GValue.of(GMETA_NAME, META_VALUE)).asAdmin();
        Collection<GValue<?>> gValues = ((AddPropertyStepPlaceholder) traversal.getSteps().get(0)).getGValues();
        assertEquals(2, gValues.size());
        assertTrue(gValues.stream().map(GValue::getName).collect(Collectors.toList()).containsAll(List.of(GNAME, GMETA_NAME)));
    }

    @Test
    public void getGValuesShouldReturnOnlyGValues() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PNAME, PVALUE, META_NAME, GValue.of(GMETA_NAME, META_VALUE)).asAdmin();
        Collection<GValue<?>> gValues = ((AddPropertyStepPlaceholder) traversal.getSteps().get(0)).getGValues();
        assertEquals(1, gValues.size());
        assertEquals(GMETA_NAME, gValues.iterator().next().getName());
    }
    
    @Test
    public void getGValuesNoneShouldReturnEmptyCollection() {
        final GraphTraversal.Admin<Object, Object> traversal = __.property(PNAME, PVALUE, META_NAME, META_VALUE).asAdmin();
        assertTrue(((AddPropertyStepPlaceholder) traversal.getSteps().get(0)).getGValues().isEmpty());
    }
}
