package com.tinkerpop.gremlin.giraph.structure.io;

import org.apache.hadoop.mapreduce.InputFormat;

/**
 * Giraph maintains its own VertexInputFormat class. These are not compatible with native Hadoop.
 * A GiraphGremlinInputFormat extends VertexInputFormat and provides Hadoop access to an InputFormat that can read:
 * &lt;NullWritable,GiraphVertex&gt; streams.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GiraphGremlinInputFormat {

    public Class<InputFormat> getInputFormatClass();
}
