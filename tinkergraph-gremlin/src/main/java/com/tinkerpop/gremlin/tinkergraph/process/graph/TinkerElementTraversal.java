package com.tinkerpop.gremlin.tinkergraph.process.graph;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.util.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.map.TinkerGraphStep;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerElementTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    private final Class<? extends Element> elementClass;
    private final Object id;

    public TinkerElementTraversal(final Element element, final TinkerGraph graph) {
        super(graph);
        this.elementClass = element.getClass();
        this.id = element.id();
        this.addStep(new StartStep<>(this, element));
    }

    public GraphTraversal<S, E> submit(final GraphComputer computer) {
        TinkerHelper.prepareTraversalForComputer(this);
        final String label = this.getSteps().get(0).getAs();
        TraversalHelper.removeStep(0, this);
        final Step identityStep = new IdentityStep(this);
        if (TraversalHelper.isLabeled(label))
            identityStep.setAs(label);

        TraversalHelper.insertStep(identityStep, 0, this);
        TraversalHelper.insertStep(new HasStep(this, new HasContainer(Element.ID, Compare.EQUAL, this.id)), 0, this);
        TraversalHelper.insertStep(new TinkerGraphStep<>(this, this.elementClass, null), 0, this);
        return super.submit(computer);
    }
}
