package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SafeStrategyTest extends AbstractGremlinTest {
    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithJustSafeStrategy() {
        g.strategy(SafeStrategy.instance());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldConstructStrategyGraphInSafeMode() {
        g.strategy(SafeStrategy.instance(), IdentityStrategy.instance()).getBaseGraph();
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldConstructStrategyVertexInSafeMode() {
        final StrategyGraph sg = g.strategy(SafeStrategy.instance(), IdentityStrategy.instance());
        ((StrategyVertex) sg.addVertex()).getBaseVertex();
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldConstructStrategyElementInSafeMode() {
        final StrategyGraph sg = g.strategy(SafeStrategy.instance(), IdentityStrategy.instance());
        ((StrategyVertex) sg.addVertex()).getBaseElement();
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldConstructStrategyVertexPropertyInSafeMode() {
        final StrategyGraph sg = g.strategy(SafeStrategy.instance(), IdentityStrategy.instance());
        ((StrategyVertexProperty) sg.addVertex().property("name", "stephen")).getBaseVertexProperty();
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldConstructStrategyEdgeInSafeMode() {
        final StrategyGraph sg = g.strategy(SafeStrategy.instance(), IdentityStrategy.instance());
        final Vertex v = sg.addVertex();
        ((StrategyEdge) v.addEdge("self", v)).getBaseEdge();
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldConstructStrategyPropertyInSafeMode() {
        final StrategyGraph sg = g.strategy(SafeStrategy.instance(), IdentityStrategy.instance());
        final Vertex v = sg.addVertex();
        ((StrategyProperty) v.addEdge("self", v).property("color", "red")).getBaseProperty();
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldConstructStrategyGraphVariablesInSafeMode() {
        final StrategyGraph sg = g.strategy(SafeStrategy.instance(), IdentityStrategy.instance());
        ((StrategyVariables) sg.variables()).getBaseVariables();
    }

    @Test
    public void shouldRemoveSafeStrategyWhenConstructingStrategyGraph() {
        assertEquals(IdentityStrategy.instance(), g.strategy(SafeStrategy.instance(), IdentityStrategy.instance()).getStrategy());
    }
}
