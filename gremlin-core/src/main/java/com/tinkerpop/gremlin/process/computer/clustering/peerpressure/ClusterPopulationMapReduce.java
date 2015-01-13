package com.tinkerpop.gremlin.process.computer.clustering.peerpressure;

import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ClusterPopulationMapReduce extends StaticMapReduce<Serializable, Long, Serializable, Long, Map<Serializable, Long>> {

    public static final String CLUSTER_POPULATION_MEMORY_KEY = "gremlin.clusterPopulationMapReduce.memoryKey";
    public static final String DEFAULT_MEMORY_KEY = "clusterPopulation";

    private String memoryKey = DEFAULT_MEMORY_KEY;

    private ClusterPopulationMapReduce() {
    }

    private ClusterPopulationMapReduce(final String memoryKey) {
        this.memoryKey = memoryKey;
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.setProperty(CLUSTER_POPULATION_MEMORY_KEY, this.memoryKey);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.memoryKey = configuration.getString(CLUSTER_POPULATION_MEMORY_KEY, DEFAULT_MEMORY_KEY);
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
    public Map<Serializable, Long> generateFinalResult(final Iterator<KeyValue<Serializable, Long>> keyValues) {
        final Map<Serializable, Long> clusterPopulation = new HashMap<>();
        keyValues.forEachRemaining(pair -> clusterPopulation.put(pair.getKey(), pair.getValue()));
        return clusterPopulation;
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

        public ClusterPopulationMapReduce create() {
            return new ClusterPopulationMapReduce(this.memoryKey);
        }

    }
}