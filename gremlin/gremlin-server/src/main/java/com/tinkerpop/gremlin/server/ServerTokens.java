package com.tinkerpop.gremlin.server;

import java.util.Arrays;
import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ServerTokens {
    public static final String OPS_SHOW = "show";
    public static final String OPS_EVAL = "eval";
    public static final String OPS_IMPORT = "import";
    public static final String OPS_INVALID = "invalid";
    public static final String OPS_RESET = "reset";
    public static final String OPS_USE = "use";
    public static final String OPS_VERSION = "version";

    public static final String ARGS_ACCEPT = "accept";
    public static final String ARGS_BINDINGS = "bindings";
    public static final String ARGS_COORDINATES = "coordinates";
    public static final String ARGS_GREMLIN = "gremlin";
    public static final String ARGS_IMPORTS = "imports";
    public static final String ARGS_INFO_TYPE = "infoType";
    public static final String ARGS_LANGUAGE = "language";
    public static final String ARGS_VERBOSE = "verbose";

    public static final String ARGS_COORDINATES_GROUP = "group";
    public static final String ARGS_COORDINATES_ARTIFACT = "artifact";
    public static final String ARGS_COORDINATES_VERSION = "version";

    public static final String ARGS_INFO_TYPE_DEPDENENCIES = "dependencies";
    public static final String ARGS_INFO_TYPE_VARIABLES = "variables";
    public static final String ARGS_INFO_TYPE_IMPORTS = "imports";

    public static final List<String> INFO_TYPES = Arrays.asList(ARGS_INFO_TYPE_DEPDENENCIES,
                                                                ARGS_INFO_TYPE_VARIABLES,
                                                                ARGS_INFO_TYPE_IMPORTS);
}
