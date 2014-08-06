package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupByStep;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupByMapReduce implements MapReduce<Object, Collection, Object, Object, Map> {

    public static final String GROUP_BY_STEP_SIDE_EFFECT_KEY = "gremlin.groupByStep.sideEffectKey";
    public static final String GROUP_BY_REDUCE_FUNCTION = "gremlin.groupByStep.reduceFunction";

    private String sideEffectKey;
    private SFunction reduceFunction;

    public GroupByMapReduce() {

    }

    public GroupByMapReduce(final GroupByStep step) {
        this.sideEffectKey = step.getAs();
        this.reduceFunction = step.reduceFunction;
    }

    @Override
    public void storeState(final Configuration configuration) {
        try {
            configuration.setProperty(GROUP_BY_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
            VertexProgramHelper.serialize(this.reduceFunction, configuration, GROUP_BY_REDUCE_FUNCTION);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

    }

    @Override
    public void loadState(final Configuration configuration) {
        try {
            this.sideEffectKey = configuration.getString(GROUP_BY_STEP_SIDE_EFFECT_KEY);
            this.reduceFunction = VertexProgramHelper.deserialize(configuration, GROUP_BY_REDUCE_FUNCTION);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public boolean doStage(final Stage stage) {
        return !stage.equals(Stage.COMBINE);
    }

    @Override
    public void map(Vertex vertex, MapEmitter<Object, Collection> emitter) {
        final HashMap<Object, Collection> tempMap = vertex.<HashMap<Object, Collection>>property(Graph.Key.hide(sideEffectKey)).orElse(new HashMap<>());
        tempMap.forEach((k, v) -> emitter.emit(k, v));
    }

    @Override
    public void reduce(final Object key, final Iterator<Collection> values, final ReduceEmitter<Object, Object> emitter) {
        final List list = new ArrayList();
        values.forEachRemaining(list::addAll);
        emitter.emit(key, (null == reduceFunction) ? list : reduceFunction.apply(list));
    }

    @Override
    public Map generateSideEffect(Iterator<Pair<Object, Object>> keyValues) {
        final Map map = new HashMap();
        keyValues.forEachRemaining(pair -> map.put(pair.getValue0(), pair.getValue1()));
        return map;
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }
}