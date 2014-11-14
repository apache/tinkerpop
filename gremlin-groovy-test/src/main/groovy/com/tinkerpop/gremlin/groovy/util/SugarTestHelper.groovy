package com.tinkerpop.gremlin.groovy.util

import com.tinkerpop.gremlin.GraphProvider
import org.codehaus.groovy.runtime.InvokerHelper

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class SugarTestHelper {

    /**
     * Clear the metaclass registry to "turn-off" sugar.
     */
    public static void clearRegistry(final GraphProvider graphProvider) {
        MetaRegistryUtil.clearRegistry(graphProvider.getImplementations())
    }
}
