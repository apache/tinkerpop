package com.tinkerpop.gremlin.hadoop.process.computer;

import com.tinkerpop.gremlin.hadoop.process.computer.util.GremlinWritable;
import com.tinkerpop.gremlin.hadoop.process.computer.util.MapReduceHelper;
import com.tinkerpop.gremlin.hadoop.structure.hdfs.VertexWritable;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopMap extends Mapper<NullWritable, VertexWritable, GremlinWritable, GremlinWritable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopMap.class);
    private MapReduce mapReduce;
    private final HadoopMapEmitter<GremlinWritable, GremlinWritable> mapEmitter = new HadoopMapEmitter<>();

    private HadoopMap() {

    }

    @Override
    public void setup(final Mapper<NullWritable, VertexWritable, GremlinWritable, GremlinWritable>.Context context) {
        this.mapReduce = MapReduceHelper.getMapReduce(context.getConfiguration());
    }

    @Override
    public void map(final NullWritable key, final VertexWritable value, final Mapper<NullWritable, VertexWritable, GremlinWritable, GremlinWritable>.Context context) throws IOException, InterruptedException {
        this.mapEmitter.setContext(context);
        this.mapReduce.map(value.get(), this.mapEmitter);
    }

    public class HadoopMapEmitter<K, V> implements MapReduce.MapEmitter<K, V> {

        private Mapper<NullWritable, VertexWritable, GremlinWritable, GremlinWritable>.Context context;
        private final GremlinWritable<K> keyWritable = new GremlinWritable<>();
        private final GremlinWritable<V> valueWritable = new GremlinWritable<>();

        public void setContext(final Mapper<NullWritable, VertexWritable, GremlinWritable, GremlinWritable>.Context context) {
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
