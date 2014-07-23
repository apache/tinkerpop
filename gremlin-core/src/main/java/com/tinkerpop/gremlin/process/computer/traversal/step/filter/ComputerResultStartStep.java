package com.tinkerpop.gremlin.process.computer.traversal.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ComputerResultStartStep<S> extends AbstractStep<S, S> {

    private final Iterator<Traverser<S>> traversers;
    private final Graph graph;
    private final Traversal computerTraversal;

    public ComputerResultStartStep(final Traversal traversal, final Graph graph, final Iterator<Traverser<S>> traversers, final Traversal computerTraversal) {
        super(traversal);
        this.traversers = traversers;
        this.graph = graph;
        this.computerTraversal = computerTraversal;

    }

    public ComputerResultStartStep(final Traversal traversal, final Iterator<Traverser<S>> traversers) {
        this(traversal, null, traversers, null);
    }

    public Traverser<S> processNextStart() {
        final Traverser<S> traverser = this.traversers.next();
        if (null != this.graph && traverser.get() instanceof Element) {
            final Element element = (Element) traverser.get();
            traverser.set(element instanceof Vertex ? (S) this.graph.v(element.id()) : (S) this.graph.e(element.id()));
        }
        return traverser;
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.computerTraversal);
    }
}
