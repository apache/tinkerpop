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
package org.apache.tinkerpop.gremlin.process.util;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.LambdaFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Compare;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

public class TraversalHelperTest {

    @Test
    public void shouldChainTogetherStepsWithNextPreviousInALinkedListStructure() {
        Traversal.Admin traversal = new DefaultTraversal<>(EmptyGraph.instance());
        traversal.asAdmin().addStep(new IdentityStep(traversal));
        traversal.asAdmin().addStep(new HasStep(traversal));
        traversal.asAdmin().addStep(new LambdaFilterStep(traversal, traverser -> true));
        validateToyTraversal(traversal);
    }

    @Test
    public void shouldAddStepsCorrectly() {
        Traversal.Admin traversal = new DefaultTraversal<>(EmptyGraph.instance());
        traversal.asAdmin().addStep(0, new LambdaFilterStep(traversal, traverser -> true));
        traversal.asAdmin().addStep(0, new HasStep(traversal));
        traversal.asAdmin().addStep(0, new IdentityStep(traversal));
        validateToyTraversal(traversal);

        traversal = new DefaultTraversal<>(EmptyGraph.instance());
        traversal.asAdmin().addStep(0, new IdentityStep(traversal));
        traversal.asAdmin().addStep(1, new HasStep(traversal));
        traversal.asAdmin().addStep(2, new LambdaFilterStep(traversal, traverser -> true));
        validateToyTraversal(traversal);
    }

    @Test
    public void shouldRemoveStepsCorrectly() {
        Traversal.Admin traversal = new DefaultTraversal<>(EmptyGraph.instance());
        traversal.asAdmin().addStep(new IdentityStep(traversal));
        traversal.asAdmin().addStep(new HasStep(traversal));
        traversal.asAdmin().addStep(new LambdaFilterStep(traversal, traverser -> true));

        traversal.asAdmin().addStep(new PropertiesStep(traversal, PropertyType.VALUE, "marko"));
        traversal.asAdmin().removeStep(3);
        validateToyTraversal(traversal);

        traversal.asAdmin().addStep(0, new PropertiesStep(traversal, PropertyType.PROPERTY, "marko"));
        traversal.asAdmin().removeStep(0);
        validateToyTraversal(traversal);

        traversal.asAdmin().removeStep(1);
        traversal.asAdmin().addStep(1, new HasStep(traversal));
        validateToyTraversal(traversal);
    }

    private static void validateToyTraversal(final Traversal traversal) {
        assertEquals(traversal.asAdmin().getSteps().size(), 3);

        assertEquals(IdentityStep.class, traversal.asAdmin().getSteps().get(0).getClass());
        assertEquals(HasStep.class, traversal.asAdmin().getSteps().get(1).getClass());
        assertEquals(LambdaFilterStep.class, traversal.asAdmin().getSteps().get(2).getClass());

        // IDENTITY STEP
        assertEquals(EmptyStep.class, ((Step) traversal.asAdmin().getSteps().get(0)).getPreviousStep().getClass());
        assertEquals(HasStep.class, ((Step) traversal.asAdmin().getSteps().get(0)).getNextStep().getClass());
        assertEquals(LambdaFilterStep.class, ((Step) traversal.asAdmin().getSteps().get(0)).getNextStep().getNextStep().getClass());
        assertEquals(EmptyStep.class, ((Step) traversal.asAdmin().getSteps().get(0)).getNextStep().getNextStep().getNextStep().getClass());

        // HAS STEP
        assertEquals(IdentityStep.class, ((Step) traversal.asAdmin().getSteps().get(1)).getPreviousStep().getClass());
        assertEquals(EmptyStep.class, ((Step) traversal.asAdmin().getSteps().get(1)).getPreviousStep().getPreviousStep().getClass());
        assertEquals(LambdaFilterStep.class, ((Step) traversal.asAdmin().getSteps().get(1)).getNextStep().getClass());
        assertEquals(EmptyStep.class, ((Step) traversal.asAdmin().getSteps().get(1)).getNextStep().getNextStep().getClass());

        // FILTER STEP
        assertEquals(HasStep.class, ((Step) traversal.asAdmin().getSteps().get(2)).getPreviousStep().getClass());
        assertEquals(IdentityStep.class, ((Step) traversal.asAdmin().getSteps().get(2)).getPreviousStep().getPreviousStep().getClass());
        assertEquals(EmptyStep.class, ((Step) traversal.asAdmin().getSteps().get(2)).getPreviousStep().getPreviousStep().getPreviousStep().getClass());
        assertEquals(EmptyStep.class, ((Step) traversal.asAdmin().getSteps().get(2)).getNextStep().getClass());

        assertEquals(3, traversal.asAdmin().getSteps().size());
    }

    @Test
    public void shouldTruncateLongName() {
        Step s = Mockito.mock(Step.class);
        Mockito.when(s.toString()).thenReturn("0123456789");
        assertEquals("0123...", TraversalHelper.getShortName(s, 7));
    }
}
