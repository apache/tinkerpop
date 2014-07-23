package com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountComputerMapReduce implements MapReduce<Object, Long, Object, Long, Map<Object, Long>> {

    public static final String GROUP_COUNT_STEP_VARIABLE = "gremlin.groupCountStep.variable";

    private String variable;

    public GroupCountComputerMapReduce() {

    }

    public GroupCountComputerMapReduce(final GroupCountComputerStep step) {
        this.variable = step.getVariable();
    }

    @Override
    public void stageConfiguration(final Configuration configuration) {
        configuration.setProperty(GROUP_COUNT_STEP_VARIABLE, this.variable);
    }

    public void setup(final Configuration configuration) {
        this.variable = configuration.getString(GROUP_COUNT_STEP_VARIABLE);
    }

    public String getResultVariable() {
        return variable;
    }

    public boolean doReduce() {
        return true;
    }

    public void map(final Vertex vertex, final MapEmitter<Object, Long> emitter) {
        final Property<Map<Object, Long>> mapProperty = vertex.property(Graph.Key.hidden(variable));
        if (mapProperty.isPresent())
            mapProperty.value().forEach((k, v) -> emitter.emit(k, v));
    }

    public void reduce(final Object key, final Iterator<Long> values, final ReduceEmitter<Object, Long> emitter) {
        long counter = 0;
        while (values.hasNext()) {
            counter = counter + values.next();
        }
        emitter.emit(key, counter);
    }

    public Map<Object, Long> getResult(final Iterator<Pair<Object, Long>> keyValues) {
        final Map<Object, Long> result = new HashMap<>();
        keyValues.forEachRemaining(pair -> result.put(pair.getValue0(), pair.getValue1()));
        return result;
    }
}
