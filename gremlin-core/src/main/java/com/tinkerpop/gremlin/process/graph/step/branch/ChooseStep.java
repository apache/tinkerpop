package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.util.ComputerAwareStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A step which offers a choice of two or more Traversals to take.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ChooseStep<S, E, M> extends ComputerAwareStep<S, E> implements TraversalHolder<S, E> {

    private static final Nest[] NEST_OPERATIONS = new Nest[]{Nest.SET_HOLDER, Nest.MERGE_IN_SIDE_EFFECTS, Nest.SET_SIDE_EFFECTS, Nest.SET_STRATEGIES};

    private final Function<S, M> mapFunction;
    private Map<M, Traversal<S, E>> choices;
    private boolean first = true;

    public ChooseStep(final Traversal traversal, final Predicate<S> predicate, final Traversal<S, E> trueChoice, final Traversal<S, E> falseChoice) {
        this(traversal,
                (Function) s -> predicate.test((S) s),
                new HashMap() {{
                    put(Boolean.TRUE, trueChoice);
                    put(Boolean.FALSE, falseChoice);
                }});
    }

    public ChooseStep(final Traversal traversal, final Function<S, M> mapFunction, final Map<M, Traversal<S, E>> choices) {
        super(traversal);
        this.mapFunction = mapFunction;
        this.choices = choices;
        this.executeTraversalOperations(NEST_OPERATIONS);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getTraversalRequirements();
    }

    @Override
    public List<Traversal<S, E>> getTraversals() {
        return Collections.unmodifiableList(new ArrayList<>(this.choices.values()));
    }

    @Override
    protected Iterator<Traverser<E>> standardAlgorithm() {
        while (true) {
            final Traverser<S> start = this.starts.next();
            final Traversal<S, E> choice = this.choices.get(this.mapFunction.apply(start.get()));
            if (null != choice) {
                choice.asAdmin().addStart(start);
                return TraversalHelper.getEnd(choice);
            }
        }
    }

    @Override
    protected Iterator<Traverser<E>> computerAlgorithm() {
        if (this.first) {
            this.first = false;
            for (final Traversal<S, E> choice : this.choices.values()) {
                TraversalHelper.getEnd(choice).setNextStep(this.getNextStep());
            }
        }
        final List<Traverser<E>> ends = new ArrayList<>();
        while (ends.isEmpty()) {
            final Traverser<S> start = this.starts.next();
            final Traversal<S, E> choice = this.choices.get(this.mapFunction.apply(start.get()));
            if (null != choice) {
                start.asAdmin().setFuture(TraversalHelper.getStart(choice).getLabel());
                ends.add((Traverser) start);
            }
        }
        return ends.iterator();
    }

    @Override
    public ChooseStep<S, E, M> clone() throws CloneNotSupportedException {
        final ChooseStep<S, E, M> clone = (ChooseStep<S, E, M>) super.clone();
        clone.choices = new HashMap<>();
        for (final Map.Entry<M, Traversal<S, E>> entry : this.choices.entrySet()) {
            clone.choices.put(entry.getKey(), entry.getValue().clone());
        }
        clone.executeTraversalOperations(NEST_OPERATIONS);
        return clone;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.choices.toString());
    }

    @Override
    public void reset() {
        super.reset();
        this.resetTraversals();
    }
}
