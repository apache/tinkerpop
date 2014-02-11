package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Optimizers;
import com.tinkerpop.gremlin.process.Traversal;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultOptimizers implements Optimizers {

    private final List<Optimizer> optimizers = new ArrayList<>();

    public List<Optimizer> get() {
        return this.optimizers;
    }

    public void register(final Optimizer optimizer) {
        this.optimizers.add(optimizer);
    }

    public void doFinalOptimizers(final Traversal pipeline) {
        this.optimizers.stream()
                .filter(o -> o instanceof Optimizer.FinalOptimizer)
                .forEach(o -> ((Optimizer.FinalOptimizer) o).optimize(pipeline));
    }


}
