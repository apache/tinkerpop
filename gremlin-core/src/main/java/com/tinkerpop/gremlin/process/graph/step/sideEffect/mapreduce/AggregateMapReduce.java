package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateStep;
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
public class AggregateMapReduce implements MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, List<Object>> {

    public static final String AGGREGATE_STEP_VARIABLE = "gremlin.aggregateStep.variable";

    private String variable;

    public AggregateMapReduce() {

    }

    public AggregateMapReduce(final AggregateStep step) {
        this.variable = step.getVariable();
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(AGGREGATE_STEP_VARIABLE, this.variable);
    }

    public void loadState(final Configuration configuration) {
        this.variable = configuration.getString(AGGREGATE_STEP_VARIABLE);
    }

    public String getSideEffectKey() {
        return variable;
    }

    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP);
    }

    public void map(final Vertex vertex, final MapEmitter<NullObject, Object> emitter) {
        final Property<Collection> mapProperty = vertex.property(Graph.Key.hidden(variable));
        if (mapProperty.isPresent())
            mapProperty.value().forEach(object -> emitter.emit(NullObject.get(), object));
    }

    public List<Object> generateSideEffect(final Iterator<Pair<NullObject, Object>> keyValues) {
        final List<Object> result = new ArrayList<>();
        keyValues.forEachRemaining(pair -> result.add(pair.getValue1()));
        return result;
    }
}
