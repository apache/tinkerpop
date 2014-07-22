package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphReduce extends Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable> {

    private MapReduce mapReduce;

    public GiraphReduce() {

    }

    @Override
    public void setup(final Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable>.Context context) {
        try {
            this.mapReduce = context.getConfiguration().getClass("MapReduce", MapReduce.class, MapReduce.class).getConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void reduce(final KryoWritable key, final Iterable<KryoWritable> values, final Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable>.Context context) throws IOException, InterruptedException {
        final Iterator<KryoWritable> itty = values.iterator();
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

}
