package com.tinkerpop.gremlin.process.computer.traversal.step.map;

import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce.TraverserMapReduce;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.detached.Attachable;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ComputerResultStep<S> extends AbstractStep<S, S> {

    private final Iterator<Traverser.Admin<S>> traversers;
    private final Graph graph;
    private final Memory memory;
    private final Traversal computerTraversal;
    private final boolean attachElements; // should be part of graph computer with "propagate properties"

    public ComputerResultStep(final Traversal traversal, final ComputerResult result, final TraversalVertexProgram traversalVertexProgram, final boolean attachElements) {
        super(traversal);
        this.graph = result.graph();
        this.memory = result.memory();
        this.attachElements = attachElements;
        this.memory.keys().forEach(key -> traversal.sideEffects().set(key, this.memory.get(key)));

        final Configuration configuration = new BaseConfiguration();
        traversalVertexProgram.storeState(configuration);
        this.computerTraversal = TraversalVertexProgram.getTraversalSupplier(configuration).get();
        final Step endStep = TraversalHelper.getEnd(this.computerTraversal);
        this.traversers = endStep instanceof SideEffectCapStep ?
                new SingleIterator<>(new SimpleTraverser<>((S) this.memory.get(((SideEffectCapStep) endStep).getSideEffectKey()), this.traversal.sideEffects())) :
                (Iterator<Traverser.Admin<S>>) this.memory.get(TraverserMapReduce.TRAVERSERS);

    }

    @Override
    public Traverser<S> processNextStart() {
        final Traverser.Admin<S> traverser = this.traversers.next();
        if (this.attachElements && (traverser.get() instanceof Attachable))
            traverser.set((S) ((Attachable) traverser.get()).attach(this.graph));
        return traverser;
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.computerTraversal);
    }
}
