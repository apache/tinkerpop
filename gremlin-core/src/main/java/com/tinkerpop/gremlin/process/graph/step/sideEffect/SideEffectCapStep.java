package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectRegistrar;
import com.tinkerpop.gremlin.process.graph.step.util.SupplyingBarrierStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.traversal.TraversalHelper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SideEffectCapStep<S, E> extends SupplyingBarrierStep<S, E> implements SideEffectRegistrar {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.SIDE_EFFECTS,
            TraverserRequirement.OBJECT
    ));

    private List<String> sideEffectKeys;

    public SideEffectCapStep(final Traversal traversal, final String... sideEffectKeys) {
        super(traversal);
        this.sideEffectKeys = Arrays.asList(sideEffectKeys);
    }

    public void registerSideEffects() {
        if (this.sideEffectKeys.isEmpty())
            this.sideEffectKeys = Arrays.asList(((SideEffectCapable) this.getPreviousStep()).getSideEffectKey());
        SideEffectCapStep.generateSupplier(this);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKeys);
    }

    public List<String> getSideEffectKeys() {
        return this.sideEffectKeys;
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

    public Map<String, Object> getMapOfSideEffects() {
        final Map<String, Object> sideEffects = new HashMap<>();
        for (final String sideEffectKey : this.sideEffectKeys) {
            sideEffects.put(sideEffectKey, this.getTraversal().asAdmin().getSideEffects().get(sideEffectKey));
        }
        return sideEffects;
    }

    /////////////////////////

    private static final <S, E> void generateSupplier(final SideEffectCapStep<S, E> sideEffectCapStep) {
        sideEffectCapStep.setSupplier(() -> sideEffectCapStep.sideEffectKeys.size() == 1 ?
                sideEffectCapStep.getTraversal().asAdmin().getSideEffects().get(sideEffectCapStep.sideEffectKeys.get(0)) :
                (E) sideEffectCapStep.getMapOfSideEffects());
    }
}
