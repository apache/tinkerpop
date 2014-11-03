package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.Constants;
import org.apache.commons.configuration.BaseConfiguration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphHelper {

    public static GiraphGraph getOutputGraph(final GiraphGraph giraphGraph) {
        final BaseConfiguration newConfiguration = new BaseConfiguration();
        newConfiguration.copy(giraphGraph.configuration());
        if (giraphGraph.configuration().containsKey(Constants.GREMLIN_GIRAPH_OUTPUT_LOCATION)) {
            newConfiguration.setProperty(Constants.GREMLIN_GIRAPH_INPUT_LOCATION, giraphGraph.configuration().getOutputLocation() + "/" + Constants.SYSTEM_G);
            newConfiguration.setProperty(Constants.GREMLIN_GIRAPH_OUTPUT_LOCATION, giraphGraph.configuration().getOutputLocation() + "_");
        }
        if (giraphGraph.configuration().containsKey(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS)) {
            newConfiguration.setProperty(Constants.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, giraphGraph.configuration().getString(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS).replace("OutputFormat", "InputFormat"));
        }
        return GiraphGraph.open(newConfiguration);
    }
}
