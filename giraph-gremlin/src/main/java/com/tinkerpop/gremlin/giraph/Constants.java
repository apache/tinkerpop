package com.tinkerpop.gremlin.giraph;

import com.esotericsoftware.kryo.Kryo;
import com.tinkerpop.gremlin.giraph.process.computer.util.RuleWritable;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.kryo.GremlinKryo;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Constants {

    // public static final Kryo KRYO = new Kryo();

    public static final String CONFIGURATION = "configuration";
    public static final String GREMLIN_INPUT_LOCATION = "giraph.gremlin.inputLocation";
    public static final String GREMLIN_OUTPUT_LOCATION = "giraph.gremlin.outputLocation";
    public static final String GIRAPH_VERTEX_INPUT_FORMAT_CLASS = "giraph.vertexInputFormatClass";
    public static final String GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS = "giraph.vertexOutputFormatClass";
    public static final String GREMLIN_JARS_IN_DISTRIBUTED_CACHE = "giraph.gremlin.jarsInDistributedCache";
    public static final String SYSTEM_G = Graph.System.system("g");
    public static final String GIRAPH_GREMLIN_JOB_PREFIX = "GiraphGremlin: ";
    public static final String GIRAPH_GREMLIN_LIBS = "GIRAPH_GREMLIN_LIBS";
    public static final String DOT_JAR = ".jar";
    public static final String GREMLIN_DERIVE_MEMORY = "giraph.gremlin.deriveMemory";
    public static final String GREMLIN_MEMORY_OUTPUT_FORMAT_CLASS = "giraph.gremlin.memoryOutputFormatClass";
    public static final String SYSTEM_MEMORY = Graph.System.system("memory");
    public static final String SYSTEM_RUNTIME = Graph.System.system("giraph.gremlin.runtime");
    public static final String SYSTEM_ITERATION = Graph.System.system("giraph.gremlin.iteration");
    public static final String GREMLIN_MEMORY_KEYS = "giraph.gremlin.memoryKeys";
    public static final String MAP_REDUCE_CLASS = "giraph.gremlin.mapReduceClass";
    public static final String GREMLIN_HALT = "giraph.gremlin.halt";
    public static final String MEMORY_MAP = Graph.Key.hide("giraph.gremlin.memoryMap");

}
