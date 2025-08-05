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
