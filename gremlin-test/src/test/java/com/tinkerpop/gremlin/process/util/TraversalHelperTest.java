package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.filter.CoinStep;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.filter.TimeLimitStep;
import com.tinkerpop.gremlin.process.graph.step.map.PropertiesStep;
import com.tinkerpop.gremlin.process.graph.step.map.PropertyMapStep;
import com.tinkerpop.gremlin.process.graph.step.map.ShuffleStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.structure.PropertyType;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

public class TraversalHelperTest {

    @Test
    public void shouldCorrectlyTestIfReversible() {
        assertTrue(TraversalHelper.isReversible(__.out()));
        assertTrue(TraversalHelper.isReversible(__.outE().inV()));
        assertTrue(TraversalHelper.isReversible(__.in().in()));
        assertTrue(TraversalHelper.isReversible(__.inE().outV().outE().inV()));
        assertTrue(TraversalHelper.isReversible(__.outE().has("since").inV()));
        assertTrue(TraversalHelper.isReversible(__.outE().as("x")));

        assertFalse(TraversalHelper.isReversible(__.identity().as("a").outE().back("a")));

    }

    @Test
    public void shouldChainTogetherStepsWithNextPreviousInALinkedListStructure() {
        Traversal traversal = new DefaultTraversal<>(Object.class);
        traversal.asAdmin().addStep(new IdentityStep(traversal));
        traversal.asAdmin().addStep(new HasStep(traversal, null));
        traversal.asAdmin().addStep(new FilterStep(traversal));
        validateToyTraversal(traversal);
    }

    @Test
    public void shouldAddStepsCorrectly() {
        Traversal traversal = new DefaultTraversal<>(Object.class);
        traversal.asAdmin().addStep(0,new FilterStep(traversal));
        traversal.asAdmin().addStep(0,new HasStep(traversal, null));
        traversal.asAdmin().addStep(0,new IdentityStep(traversal));
        validateToyTraversal(traversal);

        traversal = new DefaultTraversal<>(Object.class);
        traversal.asAdmin().addStep(0,new IdentityStep(traversal));
        traversal.asAdmin().addStep(1,new HasStep(traversal, null));
        traversal.asAdmin().addStep(2,new FilterStep(traversal));
        validateToyTraversal(traversal);
    }

    @Test
    public void shouldRemoveStepsCorrectly() {
        Traversal traversal = new DefaultTraversal<>(Object.class);
        traversal.asAdmin().addStep(new IdentityStep(traversal));
        traversal.asAdmin().addStep(new HasStep(traversal, null));
        traversal.asAdmin().addStep(new FilterStep(traversal));

        traversal.asAdmin().addStep(new PropertiesStep(traversal, PropertyType.VALUE, "marko"));
        traversal.asAdmin().removeStep(3);
        validateToyTraversal(traversal);

        traversal.asAdmin().addStep(0,new PropertiesStep(traversal, PropertyType.PROPERTY, "marko"));
        traversal.asAdmin().removeStep(0);
        validateToyTraversal(traversal);

        traversal.asAdmin().removeStep(1);
        traversal.asAdmin().addStep(1,new HasStep(traversal, null));
        validateToyTraversal(traversal);
    }

    @Test
    public void shouldIsolateSteps() {
        Traversal traversal = new DefaultTraversal<>(Object.class);
        Step step1 = new IdentityStep(traversal);
        Step step2 = new TimeLimitStep<>(traversal, 100);
        Step step3 = new CoinStep<>(traversal, 0.5);
        Step step4 = new PropertyMapStep(traversal, false, PropertyType.PROPERTY, "name");
        Step step5 = new ShuffleStep<>(traversal);
        traversal.asAdmin().addStep(step1);
        traversal.asAdmin().addStep(step2);
        traversal.asAdmin().addStep(step3);
        traversal.asAdmin().addStep(step4);
        traversal.asAdmin().addStep(step5);

        List<Step> steps;

        steps = TraversalHelper.isolateSteps(step1, step5);
        assertEquals(3, steps.size());
        assertTrue(steps.contains(step2));
        assertTrue(steps.contains(step3));
        assertTrue(steps.contains(step4));

        steps = TraversalHelper.isolateSteps(step2, step5);
        assertEquals(2, steps.size());
        assertTrue(steps.contains(step3));
        assertTrue(steps.contains(step4));

        steps = TraversalHelper.isolateSteps(step3, step5);
        assertEquals(1, steps.size());
        assertTrue(steps.contains(step4));

        steps = TraversalHelper.isolateSteps(step4, step5);
        assertEquals(0, steps.size());

        steps = TraversalHelper.isolateSteps(step5, step5);
        assertEquals(0, steps.size());

        steps = TraversalHelper.isolateSteps(step5, step1);
        assertEquals(0, steps.size());

        steps = TraversalHelper.isolateSteps(step4, step1);
        assertEquals(0, steps.size());

        steps = TraversalHelper.isolateSteps(step4, step2);
        assertEquals(0, steps.size());
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
