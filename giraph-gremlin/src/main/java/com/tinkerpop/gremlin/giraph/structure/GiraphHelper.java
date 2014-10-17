package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.Constants;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphHelper {

    public static GiraphGraph getOutputGraph(final GiraphGraph giraphGraph) {
        final Configuration conf = new BaseConfiguration();
        giraphGraph.variables().getConfiguration().getKeys().forEachRemaining(key -> {
            //try {
            conf.setProperty(key, giraphGraph.variables().getConfiguration().getProperty(key));
            //} catch (Exception e) {
            // do nothing for serialization problems
            //}
        });
        if (giraphGraph.variables().getConfiguration().containsKey(Constants.GREMLIN_OUTPUT_LOCATION)) {
            conf.setProperty(Constants.GREMLIN_INPUT_LOCATION, giraphGraph.variables().getConfiguration().getOutputLocation() + "/" + Constants.SYSTEM_G);
            conf.setProperty(Constants.GREMLIN_OUTPUT_LOCATION, giraphGraph.variables().getConfiguration().getOutputLocation() + "_");
        }
        if (giraphGraph.variables().getConfiguration().containsKey(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS)) {
            // TODO: Is this sufficient?
            conf.setProperty(Constants.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, giraphGraph.variables().getConfiguration().getString(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS).replace("OutputFormat", "InputFormat"));
        }
        return GiraphGraph.open(conf);
    }
}
