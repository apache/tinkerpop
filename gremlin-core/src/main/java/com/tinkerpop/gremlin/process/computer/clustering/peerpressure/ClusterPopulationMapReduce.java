package com.tinkerpop.gremlin.process.computer.clustering.peerpressure;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ClusterPopulationMapReduce implements MapReduce<Serializable, Long, Serializable, Long, Map<Serializable, Long>> {

    public static final String CLUSTER_POPULATION_SIDE_EFFECT_KEY = "gremlin.clusterPopulationMapReduce.sideEffectKey";
    public static final String DEFAULT_SIDE_EFFECT_KEY = "clusterPopulation";

    private String sideEffectKey = DEFAULT_SIDE_EFFECT_KEY;

    public ClusterPopulationMapReduce() {
    }

    public ClusterPopulationMapReduce(final String sideEffectKey) {
        this.sideEffectKey = sideEffectKey;
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(CLUSTER_POPULATION_SIDE_EFFECT_KEY, this.sideEffectKey);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.sideEffectKey = configuration.getString(CLUSTER_POPULATION_SIDE_EFFECT_KEY, DEFAULT_SIDE_EFFECT_KEY);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Serializable, Long> emitter) {
        final Property<Serializable> cluster = vertex.property(PeerPressureVertexProgram.CLUSTER);
        if (cluster.isPresent()) {
            emitter.emit(cluster.value(), 1l);
        }
    }

    @Override
    public void combine(final Serializable key, final Iterator<Long> values, final ReduceEmitter<Serializable, Long> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Long> values, final ReduceEmitter<Serializable, Long> emitter) {
        long count = 0l;
        while (values.hasNext()) {
            count = count + values.next();
        }
        emitter.emit(key, count);
    }

    @Override
    public Map<Serializable, Long> generateFinalResult(final Iterator<Pair<Serializable, Long>> keyValues) {
        final Map<Serializable, Long> clusterPopulation = new HashMap<>();
        keyValues.forEachRemaining(pair -> clusterPopulation.put(pair.getValue0(), pair.getValue1()));
        return clusterPopulation;
    }

    @Override
    public String getMemoryKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, this.sideEffectKey);
    }
}