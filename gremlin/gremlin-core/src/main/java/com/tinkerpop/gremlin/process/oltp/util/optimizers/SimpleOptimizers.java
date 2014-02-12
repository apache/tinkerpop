package com.tinkerpop.gremlin.process.oltp.util.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Optimizers;
import com.tinkerpop.gremlin.process.Traversal;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SimpleOptimizers implements Optimizers {

    final List<Optimizer> optimizers = new ArrayList<>();

    public void register(final Optimizer optimizer) {
        this.optimizers.add(optimizer);
    }

    public List<Optimizer> get() {
        return this.optimizers;
    }

    public void doFinalOptimizers(final Traversal traversal) {
        this.optimizers.stream().filter(o -> o instanceof Optimizer.FinalOptimizer).forEach(o -> ((Optimizer.FinalOptimizer) o).optimize(traversal));
    }


}
