package com.tinkerpop.gremlin.process.computer.traversal.step.filter;

import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.SideEffects;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce.TraversalResultMapReduce;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCap;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ComputerResultStep<S> extends AbstractStep<S, S> {

    private final Iterator<Traverser<S>> traversers;
    private final Graph graph;
    private final Traversal computerTraversal;

    public ComputerResultStep(final Traversal traversal, final Graph graph, final SideEffects sideEffects, final TraversalVertexProgram traversalVertexProgram) {
        super(traversal);
        this.graph = graph;
        this.computerTraversal = (Traversal) traversalVertexProgram.getTraversalSupplier().get();
        this.computerTraversal.strategies().apply();
        final Step endStep = TraversalHelper.getEnd(this.computerTraversal);
        this.traversers = endStep instanceof SideEffectCap ?
                new SingleIterator<>(new SimpleTraverser<>((S) sideEffects.get(((SideEffectCap) endStep).getVariable()))) :
                sideEffects.get(TraversalResultMapReduce.TRAVERSERS);
    }

    public ComputerResultStep(final Traversal traversal, final SideEffects sideEffects, final TraversalVertexProgram traversalVertexProgram) {
        this(traversal, null, sideEffects, traversalVertexProgram);
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
