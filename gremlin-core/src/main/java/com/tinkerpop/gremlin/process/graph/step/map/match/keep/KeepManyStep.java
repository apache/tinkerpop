package com.tinkerpop.gremlin.process.graph.step.map.match.keep;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.MapStep;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class KeepManyStep<E> extends MapStep<Map<String, E>, Map<String, E>> {

    public final List<String> keepAsLabels;

    public KeepManyStep(final Traversal traversal, final String... keepAsLabels) {
        super(traversal);
        this.keepAsLabels = Arrays.asList(keepAsLabels);
        this.setFunction(traverser -> {
            final Map<String, E> startMap = traverser.get();
            final Map<String, E> endMap = new HashMap<>();
            for (final String as : this.keepAsLabels) {
                final E value = startMap.get(as);
                if (null != value)
                    endMap.put(as, value);
            }
            return endMap;
        });
    }
}
