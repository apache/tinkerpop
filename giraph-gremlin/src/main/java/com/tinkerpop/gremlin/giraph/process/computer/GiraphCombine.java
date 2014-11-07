package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.process.computer.util.KryoWritable;
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
public class GiraphCombine extends Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable> {

    // TODO: extend GiraphReduce for code reuse

    private static final Logger LOGGER = LoggerFactory.getLogger(GiraphReduce.class);
    private MapReduce mapReduce;

    private GiraphCombine() {

    }

    @Override
    public void setup(final Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable>.Context context) {
        this.mapReduce = MapReduceHelper.getMapReduce(context.getConfiguration());
    }

    @Override
    public void reduce(final KryoWritable key, final Iterable<KryoWritable> values, final Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable>.Context context) throws IOException, InterruptedException {
        final Iterator<KryoWritable> itty = values.iterator();
        this.mapReduce.combine(key.get(), new Iterator() {
            @Override
            public boolean hasNext() {
                return itty.hasNext();
            }

            @Override
            public Object next() {
                return itty.next().get();
            }
        }, new GiraphCombineEmitter<>(context));
    }


    public static class GiraphCombineEmitter<OK, OV> implements MapReduce.ReduceEmitter<OK, OV> {

        final Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable>.Context context;
        final KryoWritable<OK> keyWritable = new KryoWritable<>();
        final KryoWritable<OV> valueWritable = new KryoWritable<>();

        public GiraphCombineEmitter(final Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable>.Context context) {
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
