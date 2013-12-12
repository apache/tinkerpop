package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.util.GraphFactory;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphComputerTest {

    @Test
    public void shouldSpecifyFeatureSupport() throws Exception {
        final Configuration conf = BlueprintsSuite.GraphManager.get().newGraphConfiguration();
        final Graph g = GraphFactory.open(conf);
        if (!g.getFeatures().supportsComputer()) {
            try {
                g.compute();
                fail("Features say the graph computer is not supported, but it is");
            } catch (UnsupportedOperationException e) {
                assertEquals(Graph.Exceptions.graphComputerNotSupported().getMessage(), e.getMessage());
            }
        }
    }
}
