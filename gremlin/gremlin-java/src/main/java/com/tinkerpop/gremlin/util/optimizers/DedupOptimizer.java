package com.tinkerpop.gremlin.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.filter.DedupPipe;
import com.tinkerpop.gremlin.oltp.map.ElementPropertyValuePipe;
import com.tinkerpop.gremlin.oltp.map.IdentityPipe;
import com.tinkerpop.gremlin.oltp.map.ValuePipe;
import com.tinkerpop.gremlin.util.GremlinHelper;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DedupOptimizer implements Optimizer.FinalOptimizer {

    private static final List<Class> MAP_PIPES = new ArrayList<Class>(
            Arrays.asList(IdentityPipe.class,
                    ElementPropertyValuePipe.class,
                    IdentityPipe.class,
                    ValuePipe.class
            ));

    public Pipeline optimize(final Pipeline pipeline) {
        boolean done = false;
        while (!done) {
            done = true;
            Pair<Pipe, Integer> pair = new Pair<>(null, -1);
            for (int i = 0; i < pipeline.getPipes().size(); i++) {
                if (pipeline.getPipes().get(i) instanceof DedupPipe && !((DedupPipe) pipeline.getPipes().get(i)).hasUniqueFunction) {
                    for (int j = i; j >= 0; j--) {
                        if (pipeline.getPipes().get(j) instanceof ElementPropertyValuePipe) {
                            pair = new Pair<>((Pipe) pipeline.getPipes().get(i), j);
                        }
                    }
                }
            }
            if (null != pair.getValue0() && -1 != pair.getValue1()) {
                GremlinHelper.removePipe(pair.getValue0(), pipeline);
                GremlinHelper.insertPipe(new DedupPipe(pipeline), pair.getValue1(), pipeline);
                done = false;
            }
        }

        return pipeline;

    }
}
