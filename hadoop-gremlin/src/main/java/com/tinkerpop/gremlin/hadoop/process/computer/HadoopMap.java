package com.tinkerpop.gremlin.hadoop.process.computer;

import com.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import com.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import com.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopMap extends Mapper<NullWritable, VertexWritable, ObjectWritable, ObjectWritable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopMap.class);
    private MapReduce mapReduce;
    private final HadoopMapEmitter<ObjectWritable, ObjectWritable> mapEmitter = new HadoopMapEmitter<>();

    private HadoopMap() {

    }

    @Override
    public void setup(final Mapper<NullWritable, VertexWritable, ObjectWritable, ObjectWritable>.Context context) {
        this.mapReduce = MapReduce.createMapReduce(ConfUtil.makeApacheConfiguration(context.getConfiguration()));
    }

    @Override
    public void map(final NullWritable key, final VertexWritable value, final Mapper<NullWritable, VertexWritable, ObjectWritable, ObjectWritable>.Context context) throws IOException, InterruptedException {
        this.mapEmitter.setContext(context);
        this.mapReduce.map(value.get(), this.mapEmitter);
    }

    public class HadoopMapEmitter<K, V> implements MapReduce.MapEmitter<K, V> {

        private Mapper<NullWritable, VertexWritable, ObjectWritable, ObjectWritable>.Context context;
        private final ObjectWritable<K> keyWritable = new ObjectWritable<>();
        private final ObjectWritable<V> valueWritable = new ObjectWritable<>();

        public void setContext(final Mapper<NullWritable, VertexWritable, ObjectWritable, ObjectWritable>.Context context) {
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
