package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.util.Serializer;
import org.junit.Test;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONUtilTest {
    @Test
    public void shouldBeUtilityClass() throws Exception {
        TestHelper.assertIsUtilityClass(Serializer.class);
    }


}
