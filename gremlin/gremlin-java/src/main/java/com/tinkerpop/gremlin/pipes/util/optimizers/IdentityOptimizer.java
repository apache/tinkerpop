package com.tinkerpop.gremlin.pipes.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.pipes.map.IdentityPipe;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityOptimizer implements Optimizer {

    public <S, E> Pipeline<S, E> optimize(final Pipeline<S, E> pipeline) {
        final Iterator<Pipe<?, ?>> itty = pipeline.getPipes().iterator();
        while (itty.hasNext()) {
            final Pipe<?, ?> pipe = itty.next();
            if (pipe instanceof IdentityPipe && pipe.getAs().startsWith("_"))
                itty.remove();
        }
        return pipeline;
    }

    public Rate getOptimizationRate() {
        return Rate.FINAL_COMPILE_TIME;
    }
}
