package com.tinkerpop.gremlin.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.map.IdentityPipe;
import com.tinkerpop.gremlin.util.GremlinHelper;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityOptimizer implements Optimizer.FinalOptimizer {

    public Pipeline optimize(final Pipeline pipeline) {
        ((List<Pipe>) pipeline.getPipes()).stream()
                .filter(pipe -> pipe instanceof IdentityPipe && !GremlinHelper.isLabeled(pipe))
                .collect(Collectors.<Pipe>toList())
                .forEach(pipe -> GremlinHelper.removePipe(pipe, pipeline));
        return pipeline;
    }
}
