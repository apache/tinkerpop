package com.tinkerpop.gremlin.giraph.structure.io;

import org.apache.hadoop.mapreduce.InputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface NativeInputFormat {

    public Class<InputFormat> getInputFormatClass();
}
