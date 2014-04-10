package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.map.IdentityStep;
import com.tinkerpop.gremlin.process.graph.map.PropertyStep;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

public class TraversalHelperTest {

    @Test
    public void shouldChainTogetherStepsWithNextPreviousInALinkedListStructure() {
        Traversal traversal = new DefaultTraversal<>();
        traversal.addStep(new IdentityStep(traversal));
        traversal.addStep(new HasStep(traversal, null));
        traversal.addStep(new FilterStep(traversal));
        validateToyTraversal(traversal);
    }

    @Test
    public void shouldInsertCorrectly() {
        Traversal traversal = new DefaultTraversal<>();
        TraversalHelper.insertStep(new FilterStep(traversal), 0, traversal);
        TraversalHelper.insertStep(new HasStep(traversal, null), 0, traversal);
        TraversalHelper.insertStep(new IdentityStep(traversal), 0, traversal);
        validateToyTraversal(traversal);

        traversal = new DefaultTraversal<>();
        TraversalHelper.insertStep(new IdentityStep(traversal), 0, traversal);
        TraversalHelper.insertStep(new HasStep(traversal, null), 1, traversal);
        TraversalHelper.insertStep(new FilterStep(traversal), 2, traversal);
        validateToyTraversal(traversal);
    }

    @Test
    public void shouldRemovesCorrectly() {
        Traversal traversal = new DefaultTraversal<>();
        traversal.addStep(new IdentityStep(traversal));
        traversal.addStep(new HasStep(traversal, null));
        traversal.addStep(new FilterStep(traversal));

        traversal.addStep(new PropertyStep(traversal, "marko"));
        TraversalHelper.removeStep(3, traversal);
        validateToyTraversal(traversal);

        TraversalHelper.insertStep(new PropertyStep(traversal, "marko"), 0, traversal);
        TraversalHelper.removeStep(0, traversal);
        validateToyTraversal(traversal);

        TraversalHelper.removeStep(1, traversal);
        TraversalHelper.insertStep(new HasStep(traversal, null), 1, traversal);
        validateToyTraversal(traversal);
    }

    private static void validateToyTraversal(final Traversal traversal) {
        assertEquals(traversal.getSteps().size(), 3);

        assertEquals(traversal.getSteps().get(0).getClass(), IdentityStep.class);
        assertEquals(traversal.getSteps().get(1).getClass(), HasStep.class);
        assertEquals(traversal.getSteps().get(2).getClass(), FilterStep.class);

        // IDENTITY STEP
        assertEquals(((Step) traversal.getSteps().get(0)).getPreviousStep().getClass(), EmptyStep.class);
        assertEquals(((Step) traversal.getSteps().get(0)).getNextStep().getClass(), HasStep.class);
        assertEquals(((Step) traversal.getSteps().get(0)).getNextStep().getNextStep().getClass(), FilterStep.class);
        assertEquals(((Step) traversal.getSteps().get(0)).getNextStep().getNextStep().getNextStep().getClass(), EmptyStep.class);

        // HAS STEP
        assertEquals(((Step) traversal.getSteps().get(1)).getPreviousStep().getClass(), IdentityStep.class);
        assertEquals(((Step) traversal.getSteps().get(1)).getPreviousStep().getPreviousStep().getClass(), EmptyStep.class);
        assertEquals(((Step) traversal.getSteps().get(1)).getNextStep().getClass(), FilterStep.class);
        assertEquals(((Step) traversal.getSteps().get(1)).getNextStep().getNextStep().getClass(), EmptyStep.class);

        // FILTER STEP
        assertEquals(((Step) traversal.getSteps().get(2)).getPreviousStep().getClass(), HasStep.class);
        assertEquals(((Step) traversal.getSteps().get(2)).getPreviousStep().getPreviousStep().getClass(), IdentityStep.class);
        assertEquals(((Step) traversal.getSteps().get(2)).getPreviousStep().getPreviousStep().getPreviousStep().getClass(), EmptyStep.class);
        assertEquals(((Step) traversal.getSteps().get(2)).getNextStep().getClass(), EmptyStep.class);

        assertEquals(traversal.getSteps().size(), 3);
    }
}
