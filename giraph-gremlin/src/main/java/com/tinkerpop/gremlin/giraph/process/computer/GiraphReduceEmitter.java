package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphReduceEmitter<OK, OV> implements MapReduce.ReduceEmitter<OK, OV> {

    final Reducer.Context context;
    final KryoWritable<OK> keyWritable = new KryoWritable<>();
    final KryoWritable<OV> valueWritable = new KryoWritable<>();

    public GiraphReduceEmitter(final Reducer.Context context) {
        this.context = context;
    }

    public void emit(final OK key, final OV value) {
        this.keyWritable.set(key);
        this.valueWritable.set(value);
        try {
            this.context.write(this.keyWritable, this.valueWritable);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
