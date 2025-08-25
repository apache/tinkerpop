package org.apache.tinkerpop.gremlin.process.traversal.traverser;

import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public abstract class SL_TraverserTest {
    protected static final String STEP_LABEL = "testStep";
    protected static final String LOOP_NAME = "loopName";
    protected static final long BULK = 1L;
    protected static final String TEST = "test";
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    @Mock
    protected Step<String, ?> mockStep;

    @Before
    public void setUpParent() {
        when(mockStep.getTraversal()).thenReturn(EmptyTraversal.instance());
    }

    abstract NL_SL_Traverser<String> createTraverser();

    @Test
    public void shouldInitializeSingleLoopWithLoopName() {
        NL_SL_Traverser<String> traverser = createTraverser();
        traverser.initialiseLoops(STEP_LABEL, LOOP_NAME);

        assertEquals(0, traverser.loops());
        assertEquals(0, traverser.loops(STEP_LABEL));
        assertEquals(0, traverser.loops(LOOP_NAME));
        assertEquals(2, traverser.getLoopNames().size());
        assertTrue(traverser.getLoopNames().containsAll(Set.of(LOOP_NAME, STEP_LABEL)));
    }

    @Test
    public void shouldInitializeSingleLoopWithoutLoopName() {
        NL_SL_Traverser<String> traverser = createTraverser();
        traverser.initialiseLoops(STEP_LABEL, null);

        assertEquals(0, traverser.loops());
        assertEquals(0, traverser.loops(STEP_LABEL));
        assertEquals(1, traverser.getLoopNames().size());
        assertEquals(STEP_LABEL, traverser.getLoopNames().iterator().next());
    }

    @Test
    public void shouldInitializeSingleLoopWithLoopNameSameAsStepLabel() {
        NL_SL_Traverser<String> traverser = createTraverser();
        traverser.initialiseLoops(STEP_LABEL, STEP_LABEL);

        assertEquals(0, traverser.loops());
        assertEquals(0, traverser.loops(STEP_LABEL));
        assertEquals(1, traverser.getLoopNames().size());
        assertEquals(STEP_LABEL, traverser.getLoopNames().iterator().next());
    }

    @Test
    public void shouldIncrementSingleLoop() {
        NL_SL_Traverser<String> traverser = createTraverser();
        traverser.initialiseLoops(STEP_LABEL, null);
        traverser.incrLoops();
        traverser.incrLoops();

        assertEquals(2, traverser.loops());
        assertEquals(2, traverser.loops(STEP_LABEL));
    }

    @Test
    public void shouldResetSingleLoop() {
        NL_SL_Traverser<String> traverser = createTraverser();
        traverser.initialiseLoops(STEP_LABEL, null);
        traverser.incrLoops();
        assertEquals(1, traverser.loops());

        traverser.resetLoops();
        assertEquals(0, traverser.loops());
    }

    @Test
    public void shouldCloneLoopStateOnSplit() {
        NL_SL_Traverser<String> original = createTraverser();
        original.initialiseLoops(STEP_LABEL, LOOP_NAME);
        original.incrLoops();
        NL_SL_Traverser<String> clone = (NL_SL_Traverser<String>) original.split();

        assertEquals(1, original.loops());
        assertEquals(STEP_LABEL, original.getSingleLoopStepLabel());
        assertEquals(LOOP_NAME, original.getSingleLoopName());
        assertEquals(original.loops(), clone.loops());
        assertEquals(original.getSingleLoopName(), clone.getSingleLoopName());
        assertEquals(original.getSingleLoopStepLabel(), clone.getSingleLoopStepLabel());
    }

    @Test
    public void shouldReturnSingleLoopRequirement() {
        assertEquals(TraverserRequirement.SINGLE_LOOP, createTraverser().getLoopRequirement());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForInvalidLoopName() {
        NL_SL_Traverser<String> traverser = createTraverser();
        traverser.initialiseLoops(STEP_LABEL, null);
        traverser.loops("doesnotexist");
    }
}