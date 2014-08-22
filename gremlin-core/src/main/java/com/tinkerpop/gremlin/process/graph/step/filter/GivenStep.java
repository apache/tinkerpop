package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.util.function.SBiPredicate;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GivenStep extends FilterStep<Map<String, Object>> {

    public GivenStep(final Traversal traversal, final String firstKey, final String secondKey, final SBiPredicate biPredicate) {
        super(traversal);
        this.setPredicate(traverser -> {
            final Map<String, Object> map = traverser.get();
            return biPredicate.test(map.get(firstKey), map.get(secondKey));
        });
    }
}
