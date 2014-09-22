package com.tinkerpop.gremlin.giraph;

import com.esotericsoftware.kryo.Kryo;
import com.tinkerpop.gremlin.giraph.process.computer.util.RuleWritable;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Constants {

    public static final Kryo KRYO = new Kryo(); // TODO: this is not used, remove if you don't use it

    static {
        KRYO.register(RuleWritable.Rule.class, 1000000);
        KRYO.register(MapReduce.NullObject.class, 1000001);
    }

    public static final String CONFIGURATION = "configuration";
    public static final String GREMLIN_INPUT_LOCATION = "gremlin.inputLocation";
    public static final String GREMLIN_OUTPUT_LOCATION = "gremlin.outputLocation";
    public static final String GIRAPH_VERTEX_INPUT_FORMAT_CLASS = "giraph.vertexInputFormatClass";
    public static final String GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS = "giraph.vertexOutputFormatClass";
    public static final String GREMLIN_JARS_IN_DISTRIBUTED_CACHE = "gremlin.jarsInDistributedCache";
    public static final String HIDDEN_G = Graph.Key.hide("g");
    public static final String GIRAPH_GREMLIN_JOB_PREFIX = "GiraphGremlin: ";
    public static final String GIRAPH_GREMLIN_LIBS = "GIRAPH_GREMLIN_LIBS";
    public static final String DOT_JAR = ".jar";
    public static final String GREMLIN_DERIVE_MEMORY = "gremlin.deriveMemory";
    public static final String GREMLIN_MEMORY_OUTPUT_FORMAT_CLASS = "gremlin.memoryOutputFormatClass";
    public static final String HIDDEN_MEMORY = Graph.Key.hide("memory");
    public static final String RUNTIME = Graph.Key.hide("giraphGremlin.runtime");
    public static final String ITERATION = Graph.Key.hide("giraphGremlin.iteration");
    public static final String GREMLIN_MEMORY_KEYS = "gremlin.memoryKeys";
    public static final String MAP_REDUCE_CLASS = "gremlin.mapReduceClass";
    public static final String GREMLIN_HALT = "giraphGremlin.Halt";
    public static final String MEMORY_MAP = Graph.Key.hide("giraphGremlim.memoryMap");

}
