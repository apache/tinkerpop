package com.tinkerpop.gremlin.process.oltp.util.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Pipe;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.map.IdentityPipe;
import com.tinkerpop.gremlin.process.oltp.util.GremlinHelper;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityOptimizer implements Optimizer.FinalOptimizer {

    public void optimize(final Traversal pipeline) {
        ((List<Pipe>) pipeline.getPipes()).stream()
                .filter(pipe -> pipe instanceof IdentityPipe && !GremlinHelper.isLabeled(pipe))
                .collect(Collectors.<Pipe>toList())
                .forEach(pipe -> GremlinHelper.removePipe(pipe, pipeline));
    }
}
