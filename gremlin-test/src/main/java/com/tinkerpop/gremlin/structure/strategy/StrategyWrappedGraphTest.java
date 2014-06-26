package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.FeatureRequirement;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static com.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_STRING_VALUES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedGraphTest extends AbstractGremlinTest {
	@Test
	@FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
	public void shouldNotCallBaseFunctionThusNotRemovingTheVertex() throws Exception {
		final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);

		// create an ad-hoc strategy that only marks a vertex as "deleted" and removes all edges and properties
		// but doesn't actually blow it away
		swg.strategy().setGraphStrategy(new GraphStrategy() {
			@Override
			public UnaryOperator<Supplier<Void>> getRemoveElementStrategy(final Strategy.Context<? extends StrategyWrappedElement> ctx) {
				if (ctx.getCurrent() instanceof StrategyWrappedVertex) {
					return (t) -> () -> {
						final Vertex v = ((StrategyWrappedVertex) ctx.getCurrent()).getBaseVertex();
						v.bothE().remove();
						v.properties().values().forEach(Property::remove);
						v.property("deleted", true);
						return null;
					};
				} else {
					return UnaryOperator.identity();
				}
			}
		});

		final Vertex toRemove = g.addVertex("name", "pieter");
		toRemove.addEdge("likes", g.addVertex("feature", "Strategy"));

		assertEquals(1, toRemove.properties().size());
		assertEquals(1, toRemove.bothE().count());
		assertFalse(toRemove.property("deleted").isPresent());

		swg.v(toRemove.id()).remove();

		final Vertex removed = g.v(toRemove.id());
		assertNotNull(removed);
		assertEquals(1, removed.properties().size());
		assertEquals(0, removed.bothE().count());
		assertTrue(toRemove.property("deleted").isPresent());
	}


}
