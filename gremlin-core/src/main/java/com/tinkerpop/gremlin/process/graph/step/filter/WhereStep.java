package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WhereStep<E> extends FilterStep<Map<String, E>> implements TraversalHolder {

    private static final Child[] CHILD_OPERATIONs = new Child[]{Child.SET_HOLDER, Child.SET_STRATEGIES};

    private final String firstKey;
    private final String secondKey;
    private final BiPredicate biPredicate;
    private Traversal constraint;


    public WhereStep(final Traversal traversal, final String firstKey, final String secondKey, final BiPredicate<E, E> biPredicate) {
        super(traversal);
        this.firstKey = firstKey;
        this.secondKey = secondKey;
        this.biPredicate = biPredicate;
        this.constraint = null;
        WhereStep.generatePredicate(this);

    }

    public WhereStep(final Traversal traversal, final Traversal constraint) {
        super(traversal);
        this.firstKey = null;
        this.secondKey = null;
        this.biPredicate = null;
        this.constraint = constraint;
        this.constraint.asAdmin().setStrategies(this.getTraversal().asAdmin().getStrategies());
        this.executeTraversalOperations(CHILD_OPERATIONs);
        WhereStep.generatePredicate(this);
    }

    public boolean hasBiPredicate() {
        return null != this.biPredicate;
    }

    public List<Traversal> getTraversals() {
        return null == this.constraint ? Collections.emptyList() : Collections.singletonList(this.constraint);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.firstKey, this.biPredicate, this.secondKey, this.constraint);
    }

    @Override
    public WhereStep<E> clone() throws CloneNotSupportedException {
        final WhereStep<E> clone = (WhereStep<E>) super.clone();
        if (null != this.constraint) {
            clone.constraint = this.constraint.clone();
            clone.executeTraversalOperations(CHILD_OPERATIONs);
        }
        WhereStep.generatePredicate(clone);
        return clone;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return null == this.constraint ? Collections.singleton(TraverserRequirement.OBJECT) : this.getTraversalRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    public void reset() {
        super.reset();
        this.resetTraversals();
    }

    /////////////////////////

    private static final <E> void generatePredicate(final WhereStep<E> whereStep) {
        if (null == whereStep.constraint) {
            whereStep.setPredicate(traverser -> {
                final Map<String, E> map = traverser.get();
                if (!map.containsKey(whereStep.firstKey))
                    throw new IllegalArgumentException("The provided key is not in the current map: " + whereStep.firstKey);
                if (!map.containsKey(whereStep.secondKey))
                    throw new IllegalArgumentException("The provided key is not in the current map: " + whereStep.secondKey);
                return whereStep.biPredicate.test(map.get(whereStep.firstKey), map.get(whereStep.secondKey));
            });
        } else {
            final Step<?, ?> startStep = TraversalHelper.getStart(whereStep.constraint);
            final Step<?, ?> endStep = TraversalHelper.getEnd(whereStep.constraint);
            whereStep.setPredicate(traverser -> {
                final Map<String, E> map = traverser.get();
                if (!map.containsKey(startStep.getLabel().get()))
                    throw new IllegalArgumentException("The provided key is not in the current map: " + startStep.getLabel().get());
                final Object startObject = map.get(startStep.getLabel().get());
                final Object endObject;
                if (endStep.getLabel().isPresent()) {
                    if (!map.containsKey(endStep.getLabel().get()))
                        throw new IllegalArgumentException("The provided key is not in the current map: " + endStep.getLabel().get());
                    endObject = map.get(endStep.getLabel().get());
                } else
                    endObject = null;

                // TODO: Can we add LocalStep here?
                startStep.addStart(whereStep.getTraversal().asAdmin().getTraverserGenerator().generate(startObject, (Step) startStep, traverser.bulk()));
                if (null == endObject) {
                    if (whereStep.constraint.hasNext()) {
                        whereStep.constraint.asAdmin().reset();
                        return true;
                    } else {
                        return false;
                    }

                } else {
                    while (whereStep.constraint.hasNext()) {
                        if (whereStep.constraint.next().equals(endObject)) {
                            whereStep.constraint.asAdmin().reset();
                            return true;
                        }
                    }
                    return false;
                }
            });
        }
    }


}
