package com.tinkerpop.gremlin.hadoop.structure.util;

import com.tinkerpop.gremlin.hadoop.Constants;
import com.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.commons.configuration.BaseConfiguration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopHelper {

    public static HadoopGraph getOutputGraph(final HadoopGraph hadoopGraph) {
        final BaseConfiguration newConfiguration = new BaseConfiguration();
        newConfiguration.copy(hadoopGraph.configuration());
        if (hadoopGraph.configuration().containsKey(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION)) {
            newConfiguration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, hadoopGraph.configuration().getOutputLocation() + "/" + Constants.SYSTEM_G);
            newConfiguration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, hadoopGraph.configuration().getOutputLocation() + "_");
        }
        return HadoopGraph.open(newConfiguration);
    }

}
