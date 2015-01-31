package com.tinkerpop.gremlin.process.computer.traversal.step.map;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce.TraverserMapReduce;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.traversal.step.AbstractStep;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.detached.Attachable;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ComputerResultStep<S> extends AbstractStep<S, S> {

    private final Iterator<Traverser.Admin<S>> traversers;
    private final Graph graph;
    private final Memory memory;
    private final Traversal.Admin<?, ?> computerTraversal;
    private final boolean attachElements; // should be part of graph computer with "propagate properties"

    public ComputerResultStep(final Traversal.Admin traversal, final ComputerResult result, final TraversalVertexProgram traversalVertexProgram, final boolean attachElements) {
        super(traversal);
        this.graph = result.graph();
        this.memory = result.memory();
        this.attachElements = attachElements;
        this.memory.keys().forEach(key -> traversal.getSideEffects().set(key, this.memory.get(key)));
        this.computerTraversal = traversalVertexProgram.getTraversal();

        final Step endStep = this.computerTraversal.getEndStep();
        if (endStep instanceof SideEffectCapStep) {
            final List<String> sideEffectKeys = ((SideEffectCapStep<?, ?>) endStep).getSideEffectKeys();
            if (sideEffectKeys.size() == 1)
                this.traversers = IteratorUtils.of(this.computerTraversal.getTraverserGenerator().generate(this.memory.get(sideEffectKeys.get(0)), this, 1l));
            else {
                final Map<String, Object> sideEffects = new HashMap<>();
                for (final String sideEffectKey : sideEffectKeys) {
                    sideEffects.put(sideEffectKey, this.memory.get(sideEffectKey));
                }
                this.traversers = IteratorUtils.of(this.computerTraversal.getTraverserGenerator().generate((S) sideEffects, this, 1l));
            }
        } else {
            this.traversers = this.memory.get(TraverserMapReduce.TRAVERSERS);
        }
    }

    @Override
    public Traverser<S> processNextStart() {
        final Traverser.Admin<S> traverser = this.traversers.next();
        if (this.attachElements && (traverser.get() instanceof Attachable))
            traverser.set((S) ((Attachable) traverser.get()).attach(this.graph));
        return traverser;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.computerTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.computerTraversal.getTraverserRequirements();
    }
}
