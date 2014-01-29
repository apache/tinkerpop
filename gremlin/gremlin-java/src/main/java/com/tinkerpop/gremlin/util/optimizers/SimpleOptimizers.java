package com.tinkerpop.gremlin.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Optimizers;

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


}
