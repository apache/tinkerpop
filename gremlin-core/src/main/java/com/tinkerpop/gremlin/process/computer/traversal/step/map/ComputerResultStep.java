package com.tinkerpop.gremlin.process.computer.traversal.step.map;

import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.Memory;
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
    private final Memory memory;
    private final Traversal computerTraversal;
    private final boolean resolveElements; // should be part of graph computer with "propagate properties"

    public ComputerResultStep(final Traversal traversal, final ComputerResult result, final TraversalVertexProgram traversalVertexProgram, final boolean resolveElements) {
        super(traversal);
        this.graph = result.getGraph();
        this.memory = result.getMemory();
        this.resolveElements = resolveElements;

        this.computerTraversal = (Traversal) traversalVertexProgram.getTraversalSupplier().get();
        this.computerTraversal.strategies().apply();
        final Step endStep = TraversalHelper.getEnd(this.computerTraversal);
        this.traversers = endStep instanceof SideEffectCap ?
                new SingleIterator<>(new SimpleTraverser<>((S) this.memory.get(((SideEffectCap) endStep).getSideEffectKey()))) :
                (Iterator<Traverser<S>>) this.memory.get(TraversalResultMapReduce.TRAVERSERS);
    }

    @Override
    public Traverser<S> processNextStart() {
        final Traverser<S> traverser = this.traversers.next();
        if (this.resolveElements && traverser.get() instanceof Element) {
            final Element element = (Element) traverser.get();
            traverser.set(element instanceof Vertex ? (S) this.graph.v(element.id()) : (S) this.graph.e(element.id()));
        }
        return traverser;
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.computerTraversal);
    }
}
