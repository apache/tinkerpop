package com.tinkerpop.gremlin.process.graph.step.map.match.keep;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.MapStep;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class KeepOneStep<E> extends MapStep<Map<String, Object>, E> {

    public final String keepAs;

    public KeepOneStep(final Traversal traversal, final String keepAs) {
        super(traversal);
        this.keepAs = keepAs;
        this.setFunction(traverser -> (E) traverser.get().get(this.keepAs));
    }
}
