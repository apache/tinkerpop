package com.tinkerpop.gremlin.hadoop.process.computer;

import com.tinkerpop.gremlin.hadoop.process.computer.util.GremlinWritable;
import com.tinkerpop.gremlin.hadoop.process.computer.util.MapReduceHelper;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopReduce extends Reducer<GremlinWritable, GremlinWritable, GremlinWritable, GremlinWritable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopReduce.class);
    private MapReduce mapReduce;
    private final HadoopReduceEmitter<GremlinWritable, GremlinWritable> reduceEmitter = new HadoopReduceEmitter<>();

    private HadoopReduce() {

    }

    @Override
    public void setup(final Reducer<GremlinWritable, GremlinWritable, GremlinWritable, GremlinWritable>.Context context) {
        this.mapReduce = MapReduceHelper.getMapReduce(context.getConfiguration());
    }

    @Override
    public void reduce(final GremlinWritable key, final Iterable<GremlinWritable> values, final Reducer<GremlinWritable, GremlinWritable, GremlinWritable, GremlinWritable>.Context context) throws IOException, InterruptedException {
        final Iterator<GremlinWritable> itty = values.iterator();
        this.reduceEmitter.setContext(context);
        this.mapReduce.reduce(key.get(), new Iterator() {
            @Override
            public boolean hasNext() {
                return itty.hasNext();
            }

            @Override
            public Object next() {
                return itty.next().get();
            }
        }, this.reduceEmitter);
    }


    public class HadoopReduceEmitter<OK, OV> implements MapReduce.ReduceEmitter<OK, OV> {

        private Reducer<GremlinWritable, GremlinWritable, GremlinWritable, GremlinWritable>.Context context;
        private final GremlinWritable<OK> keyWritable = new GremlinWritable<>();
        private final GremlinWritable<OV> valueWritable = new GremlinWritable<>();

        public void setContext(final Reducer<GremlinWritable, GremlinWritable, GremlinWritable, GremlinWritable>.Context context) {
            this.context = context;
        }

        @Override
        public void emit(final OK key, final OV value) {
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
