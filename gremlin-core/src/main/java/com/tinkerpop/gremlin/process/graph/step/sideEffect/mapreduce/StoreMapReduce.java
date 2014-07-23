package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StoreStep;
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
public class StoreMapReduce implements MapReduce<Object, Object, Object, Object, List<Object>> {

    public static final String STORE_STEP_VARIABLE = "gremlin.storeStep.variable";

    private String variable;

    public StoreMapReduce() {

    }

    public StoreMapReduce(final StoreStep step) {
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

    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP);
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
