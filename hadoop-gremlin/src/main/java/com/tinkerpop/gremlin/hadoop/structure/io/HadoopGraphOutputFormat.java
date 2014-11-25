package com.tinkerpop.gremlin.hadoop.structure.io;

import com.tinkerpop.gremlin.hadoop.structure.hdfs.VertexWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class HadoopGraphOutputFormat extends OutputFormat<NullWritable, VertexWritable> {
}
