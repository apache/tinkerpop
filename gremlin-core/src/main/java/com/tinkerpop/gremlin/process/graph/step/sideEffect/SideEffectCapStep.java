package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectRegistrar;
import com.tinkerpop.gremlin.process.graph.step.util.SupplierBarrierStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SideEffectCapStep<S, E> extends SupplierBarrierStep<S, E> implements SideEffectRegistrar {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.SIDE_EFFECTS,
            TraverserRequirement.OBJECT
    ));

    private String sideEffectKey;

    public SideEffectCapStep(final Traversal traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
    }

    @Override
    public void registerSideEffects() {
        if (null == this.sideEffectKey)
            this.sideEffectKey = ((SideEffectCapable) this.getPreviousStep()).getSideEffectKey();
        SideEffectCapStep.generateSupplier(this);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey);
    }

    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public SideEffectCapStep<S, E> clone() throws CloneNotSupportedException {
        final SideEffectCapStep<S, E> clone = (SideEffectCapStep<S, E>) super.clone();
        SideEffectCapStep.generateSupplier(clone);
        return clone;
    }

    /////////////////////////

    private static final <S, E> void generateSupplier(final SideEffectCapStep<S, E> sideEffectCapStep) {
        sideEffectCapStep.setSupplier(() -> sideEffectCapStep.getTraversal().asAdmin().getSideEffects().get(sideEffectCapStep.sideEffectKey));
    }
}
