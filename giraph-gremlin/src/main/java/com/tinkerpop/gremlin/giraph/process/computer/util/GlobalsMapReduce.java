package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GlobalsMapReduce implements MapReduce<String, Object, String, Object, Map<String, Object>> {

    public static final String GLOBALS = "globals";
    public static final String RUNTIME = "runtime";
    public static final String ITERATION = "iteration";
    public static final String GREMLIN_GLOBAL_KEYS = "gremlin.globalKeys";

    public Set<String> globalKeys = new HashSet<>();

    public String getSideEffectKey() {
        return GLOBALS;
    }

    public GlobalsMapReduce() {

    }

    public GlobalsMapReduce(final Set<String> globalKeys) {
        this.globalKeys = globalKeys;
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(GREMLIN_GLOBAL_KEYS, this.globalKeys.toArray(new String[this.globalKeys.size()]));
    }

    public void loadState(final Configuration configuration) {
        this.globalKeys = new HashSet<>(Arrays.asList(configuration.getStringArray(GREMLIN_GLOBAL_KEYS)));
    }

    public boolean doStage(final Stage stage) {
        return true;
    }

    public void map(final Vertex vertex, final MapEmitter<String, Object> emitter) {
        for (final String globalKey : this.globalKeys) {
            final Property property = vertex.property(Graph.Key.hidden(globalKey));
            if (property.isPresent()) {
                emitter.emit(globalKey, property.value());
            }
        }
    }

    public void combine(final String key, final Iterator<Object> values, final ReduceEmitter<String, Object> emitter) {
        this.reduce(key, values, emitter);
    }

    public void reduce(final String key, final Iterator<Object> values, final ReduceEmitter<String, Object> emitter) {
        emitter.emit(key, values.next());
    }

    public Map<String, Object> generateSideEffect(final Iterator<Pair<String, Object>> keyValues) {
        final Map<String, Object> map = new HashMap<>();
        while (keyValues.hasNext()) {
            final Pair<String, Object> pair = keyValues.next();
            map.put(pair.getValue0(), pair.getValue1());
        }
        return map;
    }

}
