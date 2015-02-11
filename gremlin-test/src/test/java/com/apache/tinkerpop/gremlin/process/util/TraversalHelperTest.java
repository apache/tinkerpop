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
package com.apache.tinkerpop.gremlin.process.util;

import com.apache.tinkerpop.gremlin.process.Step;
import com.apache.tinkerpop.gremlin.process.Traversal;
import com.apache.tinkerpop.gremlin.process.graph.traversal.__;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.filter.FilterStep;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.filter.HasStep;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.map.PropertiesStep;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.IdentityStep;
import com.apache.tinkerpop.gremlin.process.traversal.step.EmptyStep;
import com.apache.tinkerpop.gremlin.process.traversal.DefaultTraversal;
import com.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.apache.tinkerpop.gremlin.structure.PropertyType;
import org.junit.Test;
import org.mockito.Mockito;

import static com.apache.tinkerpop.gremlin.process.graph.traversal.__.*;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

public class TraversalHelperTest {

    @Test
    public void shouldCorrectlyTestIfReversible() {
        assertTrue(TraversalHelper.isReversible(out().asAdmin()));
        assertTrue(TraversalHelper.isReversible(outE().inV().asAdmin()));
        assertTrue(TraversalHelper.isReversible(in().in().asAdmin()));
        assertTrue(TraversalHelper.isReversible(inE().outV().outE().inV().asAdmin()));
        assertTrue(TraversalHelper.isReversible(outE().has("since").inV().asAdmin()));
        assertTrue(TraversalHelper.isReversible(outE().as("x").asAdmin()));

        assertFalse(TraversalHelper.isReversible(__.as("a").outE().back("a").asAdmin()));

    }

    @Test
    public void shouldChainTogetherStepsWithNextPreviousInALinkedListStructure() {
        Traversal.Admin traversal = new DefaultTraversal<>(Object.class);
        traversal.asAdmin().addStep(new IdentityStep(traversal));
        traversal.asAdmin().addStep(new HasStep(traversal, null));
        traversal.asAdmin().addStep(new FilterStep(traversal));
        validateToyTraversal(traversal);
    }

    @Test
    public void shouldAddStepsCorrectly() {
        Traversal.Admin traversal = new DefaultTraversal<>(Object.class);
        traversal.asAdmin().addStep(0, new FilterStep(traversal));
        traversal.asAdmin().addStep(0, new HasStep(traversal, null));
        traversal.asAdmin().addStep(0, new IdentityStep(traversal));
        validateToyTraversal(traversal);

        traversal = new DefaultTraversal<>(Object.class);
        traversal.asAdmin().addStep(0, new IdentityStep(traversal));
        traversal.asAdmin().addStep(1, new HasStep(traversal, null));
        traversal.asAdmin().addStep(2, new FilterStep(traversal));
        validateToyTraversal(traversal);
    }

    @Test
    public void shouldRemoveStepsCorrectly() {
        Traversal.Admin traversal = new DefaultTraversal<>(Object.class);
        traversal.asAdmin().addStep(new IdentityStep(traversal));
        traversal.asAdmin().addStep(new HasStep(traversal, null));
        traversal.asAdmin().addStep(new FilterStep(traversal));

        traversal.asAdmin().addStep(new PropertiesStep(traversal, PropertyType.VALUE, "marko"));
        traversal.asAdmin().removeStep(3);
        validateToyTraversal(traversal);

        traversal.asAdmin().addStep(0, new PropertiesStep(traversal, PropertyType.PROPERTY, "marko"));
        traversal.asAdmin().removeStep(0);
        validateToyTraversal(traversal);

        traversal.asAdmin().removeStep(1);
        traversal.asAdmin().addStep(1, new HasStep(traversal, null));
        validateToyTraversal(traversal);
    }

    private static void validateToyTraversal(final Traversal traversal) {
        assertEquals(traversal.asAdmin().getSteps().size(), 3);

        assertEquals(traversal.asAdmin().getSteps().get(0).getClass(), IdentityStep.class);
        assertEquals(traversal.asAdmin().getSteps().get(1).getClass(), HasStep.class);
        assertEquals(traversal.asAdmin().getSteps().get(2).getClass(), FilterStep.class);

        // IDENTITY STEP
        assertEquals(((Step) traversal.asAdmin().getSteps().get(0)).getPreviousStep().getClass(), EmptyStep.class);
        assertEquals(((Step) traversal.asAdmin().getSteps().get(0)).getNextStep().getClass(), HasStep.class);
        assertEquals(((Step) traversal.asAdmin().getSteps().get(0)).getNextStep().getNextStep().getClass(), FilterStep.class);
        assertEquals(((Step) traversal.asAdmin().getSteps().get(0)).getNextStep().getNextStep().getNextStep().getClass(), EmptyStep.class);

        // HAS STEP
        assertEquals(((Step) traversal.asAdmin().getSteps().get(1)).getPreviousStep().getClass(), IdentityStep.class);
        assertEquals(((Step) traversal.asAdmin().getSteps().get(1)).getPreviousStep().getPreviousStep().getClass(), EmptyStep.class);
        assertEquals(((Step) traversal.asAdmin().getSteps().get(1)).getNextStep().getClass(), FilterStep.class);
        assertEquals(((Step) traversal.asAdmin().getSteps().get(1)).getNextStep().getNextStep().getClass(), EmptyStep.class);

        // FILTER STEP
        assertEquals(((Step) traversal.asAdmin().getSteps().get(2)).getPreviousStep().getClass(), HasStep.class);
        assertEquals(((Step) traversal.asAdmin().getSteps().get(2)).getPreviousStep().getPreviousStep().getClass(), IdentityStep.class);
        assertEquals(((Step) traversal.asAdmin().getSteps().get(2)).getPreviousStep().getPreviousStep().getPreviousStep().getClass(), EmptyStep.class);
        assertEquals(((Step) traversal.asAdmin().getSteps().get(2)).getNextStep().getClass(), EmptyStep.class);

        assertEquals(traversal.asAdmin().getSteps().size(), 3);
    }

    @Test
    public void shouldTruncateLongName() {
        Step s = Mockito.mock(Step.class);
        Mockito.when(s.toString()).thenReturn("0123456789");
        assertEquals("0123...", TraversalHelper.getShortName(s, 7));
    }
}
