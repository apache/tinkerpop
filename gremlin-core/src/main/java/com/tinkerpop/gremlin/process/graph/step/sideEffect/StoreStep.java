package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StoreStep<S> extends FilterStep<S> implements Reversible, Bulkable, SideEffectCapable {

    public String variable;
    public Collection store;
    public long bulkCount = 1l;
    public SFunction<S, ?> preStoreFunction;

    public StoreStep(final Traversal traversal, final String variable, final SFunction<S, ?> preStoreFunction) {
        super(traversal);
        this.variable = variable;
        this.preStoreFunction = preStoreFunction;
        this.store = this.traversal.memory().getOrCreate(this.variable, ArrayList::new);
        this.setPredicate(traverser -> {
            final Object storeObject = null == this.preStoreFunction ? traverser.get() : this.preStoreFunction.apply(traverser.get());
            for (int i = 0; i < this.bulkCount; i++) {
                this.store.add(storeObject);
            }
            return true;
        });
    }

    public StoreStep(final Traversal traversal, final String variable) {
        this(traversal, variable, null);
    }

    public void setCurrentBulkCount(final long count) {
        this.bulkCount = count;
    }

    public String toString() {
        return this.variable.equals(SideEffectCapable.CAP_KEY) ?
                super.toString() :
                TraversalHelper.makeStepString(this, this.variable);
    }

    public String getVariable() {
        return this.variable;
    }
}
