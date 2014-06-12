package com.tinkerpop.gremlin.giraph.process.computer;

import org.apache.giraph.combiner.Combiner;
import org.apache.hadoop.io.LongWritable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphCombiner extends Combiner<LongWritable, KryoWritable> {

    public void combine(final LongWritable vertexIndex, final KryoWritable originalMessage, final KryoWritable messageToCombine) {
    }

    public KryoWritable createInitialMessage() {
        return null;
    }
}
