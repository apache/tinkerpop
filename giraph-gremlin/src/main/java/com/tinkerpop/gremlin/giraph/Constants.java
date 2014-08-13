package com.tinkerpop.gremlin.giraph;

import com.esotericsoftware.kryo.Kryo;
import com.tinkerpop.gremlin.giraph.process.computer.util.RuleWritable;
import com.tinkerpop.gremlin.process.computer.MapReduce;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Constants {

    public static final Kryo KRYO = new Kryo();

    static {
        KRYO.register(RuleWritable.Rule.class, 1000000);
        KRYO.register(MapReduce.NullObject.class, 1000001);
    }
    // Do we need a static with class loading here?

    public static final String CONFIGURATION = "configuration";
    public static final String GREMLIN_INPUT_LOCATION = "gremlin.inputLocation";
    public static final String GREMLIN_OUTPUT_LOCATION = "gremlin.outputLocation";
    public static final String GIRAPH_VERTEX_INPUT_FORMAT_CLASS = "giraph.vertexInputFormatClass";
    public static final String GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS = "giraph.vertexOutputFormatClass";
    public static final String GREMLIN_JARS_IN_DISTRIBUTED_CACHE = "gremlin.jarsInDistributedCache";
    public static final String TILDA_G = "~g";
    public static final String GIRAPH_GREMLIN_JOB_PREFIX = "GiraphGremlin: ";
    public static final String GIRAPH_GREMLIN_LIBS = "GIRAPH_GREMLIN_LIBS";
    public static final String DOT_JAR = ".jar";
    public static final String GREMLIN_DERIVE_COMPUTER_SIDE_EFFECTS = "gremlin.deriveComputerSideEffects";
    public static final String GREMLIN_SIDE_EFFECT_OUTPUT_FORMAT_CLASS = "gremlin.sideEffectOutputFormatClass";
    public static final String TILDA_SIDE_EFFECTS = "~sideEffects";
    public static final String RUNTIME = "runtime";
    public static final String ITERATION = "iteration";
    public static final String GREMLIN_SIDE_EFFECT_KEYS = "gremlin.sideEffectKeys";
    public static final String MAP_REDUCE_CLASS = "gremlin.mapReduceClass";

}
