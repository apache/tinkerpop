package com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SackObjectStep<S, V> extends SideEffectStep<S> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.SACK,
            TraverserRequirement.OBJECT
    ));

    private BiFunction<V, S, V> operator;

    public SackObjectStep(final Traversal.Admin traversal, final BiFunction<V, S, V> operator) {
        super(traversal);
        this.operator = operator;
        SackObjectStep.generateConsumer(this);

    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public SackObjectStep<S, V> clone() throws CloneNotSupportedException {
        final SackObjectStep<S, V> clone = (SackObjectStep<S, V>) super.clone();
        SackObjectStep.generateConsumer(clone);
        return clone;
    }

    /////////////////////////

    private static final <S, V> void generateConsumer(final SackObjectStep<S, V> sackObjectStep) {
        sackObjectStep.setConsumer(traverser -> traverser.sack(sackObjectStep.operator.apply(traverser.sack(), traverser.get())));
    }
}
