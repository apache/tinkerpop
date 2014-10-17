package com.tinkerpop.gremlin.process.computer.clustering.peerpressure;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ClusterCountMapReduce implements MapReduce<MapReduce.NullObject, Serializable, MapReduce.NullObject, Integer, Integer> {

    public static final String CLUSTER_COUNT_SIDE_EFFECT_KEY = "gremlin.clusterCountMapReduce.sideEffectKey";
    public static final String DEFAULT_SIDE_EFFECT_KEY = "clusterCount";

    private String sideEffectKey = DEFAULT_SIDE_EFFECT_KEY;


    public ClusterCountMapReduce() {

    }

    public ClusterCountMapReduce(final String sideEffectKey) {
        this.sideEffectKey = sideEffectKey;
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(CLUSTER_COUNT_SIDE_EFFECT_KEY, this.sideEffectKey);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.sideEffectKey = configuration.getString(CLUSTER_COUNT_SIDE_EFFECT_KEY, DEFAULT_SIDE_EFFECT_KEY);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, Serializable> emitter) {
        final Property<Serializable> cluster = vertex.property(PeerPressureVertexProgram.CLUSTER);
        if (cluster.isPresent()) {
            emitter.emit(NullObject.instance(), cluster.value());
        }
    }

    @Override
    public void combine(final NullObject key, final Iterator<Serializable> values, final ReduceEmitter<NullObject, Integer> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public void reduce(final NullObject key, final Iterator<Serializable> values, final ReduceEmitter<NullObject, Integer> emitter) {
        final Set<Serializable> set = new HashSet<>();
        values.forEachRemaining(set::add);
        emitter.emit(NullObject.instance(), set.size());

    }

    @Override
    public Integer generateFinalResult(final Iterator<Pair<NullObject, Integer>> keyValues) {
        return keyValues.next().getValue1();
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