package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StoreStep<S> extends FilterStep<S> implements Reversible, Bulkable, SideEffectCapable {

    public String variable;
    public Collection collection;
    public long bulkCount = 1l;
    public FunctionRing functionRing;

    public StoreStep(final Traversal traversal, final String variable, final SFunction... storeFunctions) {
        super(traversal);
        this.variable = variable;
        this.functionRing = new FunctionRing(storeFunctions);
        this.collection = this.traversal.memory().getOrCreate(this.variable, () -> new ArrayList());
        this.setPredicate(traverser -> {
            final Object storeObject = this.functionRing.next().apply(traverser.get());
            for (int i = 0; i < this.bulkCount; i++) {
                this.collection.add(storeObject);
            }
            return true;
        });
    }

    public void setCurrentBulkCount(final long count) {
        this.bulkCount = count;
    }

}
