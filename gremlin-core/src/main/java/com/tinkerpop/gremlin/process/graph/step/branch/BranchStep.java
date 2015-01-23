package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.ForkHolder;
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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BranchStep<S, E, M> extends ComputerAwareStep<S, E> implements TraversalHolder, ForkHolder<M, S, E> {

    private static final TraversalHolder.Child[] CHILD_OPERATIONS = new TraversalHolder.Child[]{TraversalHolder.Child.SET_HOLDER, TraversalHolder.Child.MERGE_IN_SIDE_EFFECTS, TraversalHolder.Child.SET_SIDE_EFFECTS};

    protected Function<Traverser<S>, M> branchFunction;
    protected Map<M, List<Traversal<S, E>>> branches = new HashMap<>();

    public BranchStep(final Traversal traversal) {
        super(traversal);
    }

    public void setFunction(final Function<Traverser<S>, M> branchFunction) {
        this.branchFunction = branchFunction;
    }

    @Override
    public void addFork(final M pickToken, final Traversal<S, E> traversalFork) {
        if (this.branches.containsKey(pickToken))
            this.branches.get(pickToken).add(traversalFork);
        else
            this.branches.put(pickToken, new ArrayList<>(Collections.singletonList(traversalFork)));
        traversalFork.asAdmin().addStep(new EndStep(traversalFork));
        this.executeTraversalOperations(traversalFork, CHILD_OPERATIONS);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return ForkHolder.super.getRequirements();
    }

    @Override
    public List<Traversal<S, E>> getGlobalTraversals() {
        return Collections.unmodifiableList(this.branches.values().stream()
                .flatMap(list -> list.stream())
                .collect(Collectors.toList()));
    }

    @Override
    protected Iterator<Traverser<E>> standardAlgorithm() {
        while (true) {
            final Optional<Traversal<S, E>> fullBranch = this.branches.values().stream()
                    .flatMap(list -> list.stream())
                    .filter(Traversal::hasNext)
                    .findAny();
            if (fullBranch.isPresent()) return fullBranch.get().asAdmin().getEndStep();

            final Traverser.Admin<S> start = this.starts.next();
            final M choice = this.branchFunction.apply(start);
            final List<Traversal<S, E>> branch = this.branches.get(choice);
            if (null != branch)
                branch.forEach(traversal -> traversal.asAdmin().addStart(start.split()));
            // TODO: else Pick.none
            if (choice != Pick.any) {
                final List<Traversal<S, E>> anyBranch = this.branches.get(Pick.any);
                if (null != anyBranch)
                    anyBranch.forEach(traversal -> traversal.asAdmin().addStart(start.split()));
            }
        }
    }

    @Override
    protected Iterator<Traverser<E>> computerAlgorithm() {
        final List<Traverser<E>> ends = new ArrayList<>();
        final Traverser.Admin<S> start = this.starts.next();
        final M choice = this.branchFunction.apply(start);
        final List<Traversal<S, E>> branch = this.branches.get(choice);
        if (null != branch) {
            branch.forEach(traversal -> {
                final Traverser.Admin<E> split = (Traverser.Admin<E>) start.split();
                split.setStepId(traversal.asAdmin().getStartStep().getId());
                ends.add(split);
            });
        } // TODO: else Pick.none
        if (choice != Pick.any) {
            final List<Traversal<S, E>> anyBranch = this.branches.get(Pick.any);
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
        clone.branches = new HashMap<>();
        for (final Map.Entry<M, List<Traversal<S, E>>> entry : this.branches.entrySet()) {
            for (final Traversal<S, E> traversal : entry.getValue()) {
                final Traversal<S, E> clonedTraversal = traversal.clone();
                if (clone.branches.containsKey(entry.getKey()))
                    clone.branches.get(entry.getKey()).add(clonedTraversal);
                else
                    clone.branches.put(entry.getKey(), new ArrayList<>(Collections.singletonList(clonedTraversal)));
                clone.executeTraversalOperations(clonedTraversal, CHILD_OPERATIONS);
            }
        }
        return clone;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.branches.toString());
    }

    @Override
    public void reset() {
        super.reset();
        this.resetTraversals();
    }
}
