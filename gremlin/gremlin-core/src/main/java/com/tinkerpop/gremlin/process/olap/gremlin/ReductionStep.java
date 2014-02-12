package com.tinkerpop.gremlin.process.olap.gremlin;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.olap.GraphMemory;
import com.tinkerpop.gremlin.process.oltp.map.MapStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReductionStep<S> extends MapStep<S, S> {

    public GraphMemory.ReductionMemory reductionMemory;

    public ReductionStep(final Traversal traversal, final GraphMemory.ReductionMemory reductionMemory) {
        super(traversal);
        this.reductionMemory = reductionMemory;
        this.setFunction(s -> {
            this.reductionMemory.emit(s.get(), 1);
            return (S) Step.NO_OBJECT;
        });
    }
}
