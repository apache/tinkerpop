package com.tinkerpop.gremlin.giraph;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Constants {

    // public static final Kryo KRYO = new Kryo();

    public static final String GREMLIN_GIRAPH_INPUT_LOCATION = "gremlin.giraph.inputLocation";
    public static final String GREMLIN_GIRAPH_OUTPUT_LOCATION = "gremlin.giraph.outputLocation";
    public static final String GIRAPH_VERTEX_INPUT_FORMAT_CLASS = "giraph.vertexInputFormatClass";
    public static final String GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS = "giraph.vertexOutputFormatClass";
    public static final String GREMLIN_GIRAPH_JARS_IN_DISTRIBUTED_CACHE = "gremlin.giraph.jarsInDistributedCache";
    public static final String SYSTEM_G = Graph.System.system("g");
    public static final String GIRAPH_GREMLIN_JOB_PREFIX = "GiraphGremlin: ";
    public static final String GIRAPH_GREMLIN_LIBS = "GIRAPH_GREMLIN_LIBS";
    public static final String DOT_JAR = ".jar";
    public static final String GREMLIN_GIRAPH_DERIVE_MEMORY = "gremlin.giraph.deriveMemory";
    public static final String GREMLIN_GIRAPH_MEMORY_OUTPUT_FORMAT_CLASS = "gremlin.giraph.memoryOutputFormatClass";
    public static final String SYSTEM_MEMORY = Graph.System.system("memory");
    public static final String SYSTEM_RUNTIME = Graph.System.system("gremlin.giraph.runtime");
    public static final String SYSTEM_ITERATION = Graph.System.system("gremlin.giraph.iteration");
    public static final String GREMLIN_GIRAPH_MEMORY_KEYS = "gremlin.giraph.memoryKeys";
    public static final String GRELMIN_GIRAPH_MAP_REDUCE_CLASS = "gremlin.giraph.mapReduceClass";
    public static final String GREMLIN_GIRAPH_HALT = "gremlin.giraph.halt";
    public static final String MEMORY_MAP = Graph.Key.hide("gremlin.giraph.memoryMap");

}
