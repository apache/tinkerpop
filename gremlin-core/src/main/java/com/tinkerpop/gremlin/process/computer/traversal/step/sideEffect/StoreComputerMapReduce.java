package com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StoreComputerMapReduce implements MapReduce<Object, Object, Object, Object, List<Object>> {

    public static final String STORE_STEP_VARIABLE = "gremlin.storeStep.variable";

    private String variable;

    public StoreComputerMapReduce() {

    }

    public StoreComputerMapReduce(final StoreComputerStep step) {
        this.variable = step.getVariable();
    }

    @Override
    public void stageConfiguration(final Configuration configuration) {
        configuration.setProperty(STORE_STEP_VARIABLE, this.variable);
    }

    public void setup(final Configuration configuration) {
        this.variable = configuration.getString(STORE_STEP_VARIABLE);
    }

    public String getResultVariable() {
        return variable;
    }

    public boolean doReduce() {
        return false;
    }

    public void map(final Vertex vertex, final MapEmitter<Object, Object> emitter) {
        final Property<Collection> mapProperty = vertex.property(Graph.Key.hidden(variable));
        if (mapProperty.isPresent())
            mapProperty.value().forEach(object -> emitter.emit(NullObject.get(), object));
    }

    public List<Object> getResult(final Iterator<Pair<Object, Object>> keyValues) {
        final List<Object> result = new ArrayList<>();
        keyValues.forEachRemaining(pair -> result.add(pair.getValue1()));
        return result;
    }
}
