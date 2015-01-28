package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.util.function.CloneableLambda;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SackElementValueStep<S extends Element, V> extends SideEffectStep<S> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.SACK,
            TraverserRequirement.OBJECT
    ));

    private BinaryOperator<V> operator;
    private final String propertyKey;

    public SackElementValueStep(final Traversal traversal, final BinaryOperator<V> operator, final String propertyKey) {
        super(traversal);
        this.operator = operator;
        this.propertyKey = propertyKey;
        SackElementValueStep.generateConsumer(this);

    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.operator, this.propertyKey);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public SackElementValueStep<S, V> clone() throws CloneNotSupportedException {
        final SackElementValueStep<S, V> clone = (SackElementValueStep<S, V>) super.clone();
        clone.operator = CloneableLambda.tryClone(this.operator);
        SackElementValueStep.generateConsumer(clone);
        return clone;
    }

    /////////////////////////

    private static final <S extends Element, V> void generateConsumer(final SackElementValueStep<S, V> sackElementValueStep) {
        sackElementValueStep.setConsumer(traverser -> {
            traverser.get().iterators().valueIterator(sackElementValueStep.propertyKey).forEachRemaining(value -> {
                traverser.sack(sackElementValueStep.operator.apply(traverser.sack(), (V) value));
            });
        });
    }
}
