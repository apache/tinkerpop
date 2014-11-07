package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.process.computer.util.GremlinWritable;
import com.tinkerpop.gremlin.giraph.process.computer.util.MapReduceHelper;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphReduce extends Reducer<GremlinWritable, GremlinWritable, GremlinWritable, GremlinWritable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GiraphReduce.class);
    private MapReduce mapReduce;

    private GiraphReduce() {

    }

    @Override
    public void setup(final Reducer<GremlinWritable, GremlinWritable, GremlinWritable, GremlinWritable>.Context context) {
        this.mapReduce = MapReduceHelper.getMapReduce(context.getConfiguration());
    }

    @Override
    public void reduce(final GremlinWritable key, final Iterable<GremlinWritable> values, final Reducer<GremlinWritable, GremlinWritable, GremlinWritable, GremlinWritable>.Context context) throws IOException, InterruptedException {
        final Iterator<GremlinWritable> itty = values.iterator();
        this.mapReduce.reduce(key.get(), new Iterator() {
            @Override
            public boolean hasNext() {
                return itty.hasNext();
            }

            @Override
            public Object next() {
                return itty.next().get();
            }
        }, new GiraphReduceEmitter<>(context));
    }


    public static class GiraphReduceEmitter<OK, OV> implements MapReduce.ReduceEmitter<OK, OV> {

        final Reducer<GremlinWritable, GremlinWritable, GremlinWritable, GremlinWritable>.Context context;
        final GremlinWritable<OK> keyWritable = new GremlinWritable<>();
        final GremlinWritable<OV> valueWritable = new GremlinWritable<>();

        public GiraphReduceEmitter(final Reducer<GremlinWritable, GremlinWritable, GremlinWritable, GremlinWritable>.Context context) {
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
