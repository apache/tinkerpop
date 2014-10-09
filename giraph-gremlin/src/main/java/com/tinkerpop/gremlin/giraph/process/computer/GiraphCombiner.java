package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.process.computer.util.KryoWritable;
import org.apache.giraph.combiner.Combiner;
import org.apache.hadoop.io.LongWritable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
// TODO!
public final class GiraphCombiner extends Combiner<LongWritable, KryoWritable> {

    @Override
    public void combine(final LongWritable vertexIndex, final KryoWritable originalMessage, final KryoWritable messageToCombine) {
    }

    @Override
    public KryoWritable createInitialMessage() {
        return new KryoWritable<>(null);
    }
}
