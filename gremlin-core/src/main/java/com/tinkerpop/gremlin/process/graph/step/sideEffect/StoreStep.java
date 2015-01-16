package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.FunctionHolder;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.StoreMapReduce;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.BulkSet;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StoreStep<S> extends SideEffectStep<S> implements SideEffectCapable, Reversible, FunctionHolder<S, Object>, MapReducer<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.BULK,
            TraverserRequirement.OBJECT,
            TraverserRequirement.SIDE_EFFECTS
    ));

    private Function<S, Object> preStoreFunction = s -> s;
    private final String sideEffectKey;

    public StoreStep(final Traversal traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = null == sideEffectKey ? this.getLabel().orElse(this.getId()) : sideEffectKey;
        TraversalHelper.verifySideEffectKeyIsNotAStepLabel(this.sideEffectKey, this.traversal);
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, BulkSet::new);
        this.setConsumer(traverser -> TraversalHelper.addToCollection(traverser.sideEffects(this.sideEffectKey),
                null == this.preStoreFunction ? traverser.get() : this.preStoreFunction.apply(traverser.get()), traverser.bulk()));
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey);
    }

    @Override
    public MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> getMapReduce() {
        return new StoreMapReduce(this);
    }

    @Override
    public void addFunction(final Function<S, Object> function) {
        this.preStoreFunction = function;
    }

    @Override
    public List<Function<S, Object>> getFunctions() {
        return null == this.preStoreFunction ? Collections.emptyList() : Arrays.asList(this.preStoreFunction);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
