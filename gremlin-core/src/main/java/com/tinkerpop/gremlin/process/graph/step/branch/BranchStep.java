package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.marker.TraversalOptionHolder;
import com.tinkerpop.gremlin.process.graph.step.util.ComputerAwareStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.CloneableLambda;
import com.tinkerpop.gremlin.util.function.ResettableLambda;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BranchStep<S, E, M> extends ComputerAwareStep<S, E> implements TraversalOptionHolder<M, S, E> {

    private static final TraversalHolder.Child[] CHILD_OPERATIONS = new TraversalHolder.Child[]{
            TraversalHolder.Child.SET_HOLDER,
            TraversalHolder.Child.MERGE_IN_SIDE_EFFECTS,
            TraversalHolder.Child.SET_SIDE_EFFECTS};

    protected Function<Traverser<S>, M> pickFunction;
    protected Map<M, List<Traversal<S, E>>> traversalOptions = new HashMap<>();
    protected List<Traversal<S, E>> list = new ArrayList<>();

    public BranchStep(final Traversal traversal) {
        super(traversal);
    }

    public void setFunction(final Function<Traverser<S>, M> pickFunction) {
        this.pickFunction = pickFunction;
    }

    @Override
    public void addOption(final M pickToken, final Traversal<S, E> traversalOption) {
        if (this.traversalOptions.containsKey(pickToken))
            this.traversalOptions.get(pickToken).add(traversalOption);
        else
            this.traversalOptions.put(pickToken, new ArrayList<>(Collections.singletonList(traversalOption)));
        traversalOption.asAdmin().addStep(new EndStep(traversalOption));
        this.executeTraversalOperations(traversalOption, CHILD_OPERATIONS);
        list.add(traversalOption);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return TraversalOptionHolder.super.getRequirements();
    }

    @Override
    public List<Traversal<S, E>> getGlobalTraversals() {
        return Collections.unmodifiableList(this.traversalOptions.values().stream()
                .flatMap(list -> list.stream())
                .collect(Collectors.toList()));
    }

    @Override
    protected Iterator<Traverser<E>> standardAlgorithm() {
        while (true) {
            for (final List<Traversal<S, E>> options : this.traversalOptions.values()) {
                for (final Traversal<S, E> option : options) {
                    if (option.hasNext())
                        return option.asAdmin().getEndStep();
                }
            }
            ///
            final Traverser.Admin<S> start = this.starts.next();
            final M choice = this.pickFunction.apply(start);
            final List<Traversal<S, E>> branch = this.traversalOptions.containsKey(choice) ? this.traversalOptions.get(choice) : this.traversalOptions.get(Pick.none);
            if (null != branch)
                branch.forEach(traversal -> traversal.asAdmin().addStart(start.split()));
            if (choice != Pick.any) {
                final List<Traversal<S, E>> anyBranch = this.traversalOptions.get(Pick.any);
                if (null != anyBranch)
                    anyBranch.forEach(traversal -> traversal.asAdmin().addStart(start.split()));
            }
        }
    }

    @Override
    protected Iterator<Traverser<E>> computerAlgorithm() {
        final List<Traverser<E>> ends = new ArrayList<>();
        final Traverser.Admin<S> start = this.starts.next();
        final M choice = this.pickFunction.apply(start);
        final List<Traversal<S, E>> branch = this.traversalOptions.containsKey(choice) ? this.traversalOptions.get(choice) : this.traversalOptions.get(Pick.none);
        if (null != branch) {
            branch.forEach(traversal -> {
                final Traverser.Admin<E> split = (Traverser.Admin<E>) start.split();
                split.setStepId(traversal.asAdmin().getStartStep().getId());
                ends.add(split);
            });
        }
        if (choice != Pick.any) {
            final List<Traversal<S, E>> anyBranch = this.traversalOptions.get(Pick.any);
            if (null != anyBranch) {
                anyBranch.forEach(traversal -> {
                    final Traverser.Admin<E> split = (Traverser.Admin<E>) start.split();
                    split.setStepId(traversal.asAdmin().getStartStep().getId());
                    ends.add(split);
                });
            }
        }
        return ends.iterator();
    }

    @Override
    public BranchStep<S, E, M> clone() throws CloneNotSupportedException {
        final BranchStep<S, E, M> clone = (BranchStep<S, E, M>) super.clone();
        clone.traversalOptions = new HashMap<>();
        for (final Map.Entry<M, List<Traversal<S, E>>> entry : this.traversalOptions.entrySet()) {
            for (final Traversal<S, E> traversal : entry.getValue()) {
                final Traversal<S, E> clonedTraversal = traversal.clone();
                if (clone.traversalOptions.containsKey(entry.getKey()))
                    clone.traversalOptions.get(entry.getKey()).add(clonedTraversal);
                else
                    clone.traversalOptions.put(entry.getKey(), new ArrayList<>(Collections.singletonList(clonedTraversal)));
                clone.executeTraversalOperations(clonedTraversal, CHILD_OPERATIONS);
            }
        }
        clone.pickFunction = CloneableLambda.cloneOrReturn(this.pickFunction);
        return clone;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.traversalOptions.toString());
    }

    @Override
    public void reset() {
        super.reset();
        ResettableLambda.resetOrReturn(this.pickFunction);
        for (final List<Traversal<S, E>> options : this.traversalOptions.values()) {
            for (final Traversal<S, E> option : options) {
                option.asAdmin().reset();
            }
        }
    }
}
