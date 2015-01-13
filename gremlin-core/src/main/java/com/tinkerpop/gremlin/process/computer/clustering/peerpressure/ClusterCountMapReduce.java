package com.tinkerpop.gremlin.process.computer.clustering.peerpressure;

import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ClusterCountMapReduce extends StaticMapReduce<MapReduce.NullObject, Serializable, MapReduce.NullObject, Integer, Integer> {

    public static final String CLUSTER_COUNT_MEMORY_KEY = "gremlin.clusterCountMapReduce.memoryKey";
    public static final String DEFAULT_MEMORY_KEY = "clusterCount";

    private String memoryKey = DEFAULT_MEMORY_KEY;


    private ClusterCountMapReduce() {

    }

    private ClusterCountMapReduce(final String memoryKey) {
        this.memoryKey = memoryKey;
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.setProperty(CLUSTER_COUNT_MEMORY_KEY, this.memoryKey);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.memoryKey = configuration.getString(CLUSTER_COUNT_MEMORY_KEY, DEFAULT_MEMORY_KEY);
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
    public Integer generateFinalResult(final Iterator<KeyValue<NullObject, Integer>> keyValues) {
        return keyValues.next().getValue();
    }

    @Override
    public String getMemoryKey() {
        return this.memoryKey;
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, this.memoryKey);
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {

        private String memoryKey = DEFAULT_MEMORY_KEY;

        private Builder() {

        }

        public Builder memoryKey(final String memoryKey) {
            this.memoryKey = memoryKey;
            return this;
        }

        public ClusterCountMapReduce create() {
            return new ClusterCountMapReduce(this.memoryKey);
        }
    }
}