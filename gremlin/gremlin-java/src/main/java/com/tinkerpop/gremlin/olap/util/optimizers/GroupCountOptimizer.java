package com.tinkerpop.gremlin.olap.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.sideeffect.GroupCountPipe;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountOptimizer implements Optimizer.FinalOptimizer {

    public void optimize(final Pipeline pipeline) {
        // if pipeline has a GroupCountPipe, replace it with a Gremlin OLAP GroupCountPipe using ReductionMemory
        for (Pipe pipe : (List<Pipe>) pipeline.getPipes()) {
            if (pipe instanceof GroupCountPipe) {
                //pipe.get
            }
        }
    }
}