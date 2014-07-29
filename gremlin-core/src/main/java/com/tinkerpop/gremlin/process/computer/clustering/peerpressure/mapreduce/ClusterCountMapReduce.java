package com.tinkerpop.gremlin.process.computer.clustering.peerpressure.mapreduce;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ClusterCountMapReduce implements MapReduce<MapReduce.NullObject, Serializable, MapReduce.NullObject, Integer, Integer> {

    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, Serializable> emitter) {
        final Property<Serializable> cluster = vertex.property(PeerPressureVertexProgram.CLUSTER);
        if (cluster.isPresent()) {
            emitter.emit(NullObject.get(), cluster.value());
        }
    }

    public void combine(final NullObject key, final Iterator<Serializable> values, final ReduceEmitter<NullObject, Integer> emitter) {
        this.reduce(key, values, emitter);
    }

    public void reduce(final NullObject key, final Iterator<Serializable> values, final ReduceEmitter<NullObject, Integer> emitter) {
        final Set<Serializable> set = new HashSet<>();
        values.forEachRemaining(set::add);
        emitter.emit(NullObject.get(), set.size());

    }

    @Override
    public Integer generateSideEffect(final Iterator<Pair<NullObject, Integer>> keyValues) {
        return keyValues.next().getValue1();
    }

    @Override
    public String getSideEffectKey() {
        return "clusterCount";
    }
}