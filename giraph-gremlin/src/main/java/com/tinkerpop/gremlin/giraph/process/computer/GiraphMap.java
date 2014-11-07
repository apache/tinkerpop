package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.process.computer.util.GremlinWritable;
import com.tinkerpop.gremlin.giraph.process.computer.util.MapReduceHelper;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphMap extends Mapper<NullWritable, GiraphComputeVertex, GremlinWritable, GremlinWritable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GiraphMap.class);
    private MapReduce mapReduce;

    private GiraphMap() {

    }

    @Override
    public void setup(final Mapper<NullWritable, GiraphComputeVertex, GremlinWritable, GremlinWritable>.Context context) {
        this.mapReduce = MapReduceHelper.getMapReduce(context.getConfiguration());
    }

    @Override
    public void map(final NullWritable key, final GiraphComputeVertex value, final Mapper<NullWritable, GiraphComputeVertex, GremlinWritable, GremlinWritable>.Context context) throws IOException, InterruptedException {
        this.mapReduce.map(value.getBaseVertex(), new GiraphMapEmitter<>(context));
    }

    public static class GiraphMapEmitter<K, V> implements MapReduce.MapEmitter<K, V> {

        final Mapper<NullWritable, GiraphComputeVertex, GremlinWritable, GremlinWritable>.Context context;
        final GremlinWritable<K> keyWritable = new GremlinWritable<>();
        final GremlinWritable<V> valueWritable = new GremlinWritable<>();

        public GiraphMapEmitter(final Mapper<NullWritable, GiraphComputeVertex, GremlinWritable, GremlinWritable>.Context context) {
            this.context = context;
        }

        @Override
        public void emit(final K key, final V value) {
            this.keyWritable.set(key);
            this.valueWritable.set(value);
            try {
                this.context.write(this.keyWritable, this.valueWritable);
            } catch (final Exception e) {
                LOGGER.error(e.getMessage());
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }
}
