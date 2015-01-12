package com.tinkerpop.gremlin.groovy.util

import com.tinkerpop.gremlin.GraphProvider
import com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal
import com.tinkerpop.gremlin.process.traverser.PathTraverser
import com.tinkerpop.gremlin.process.traverser.SimpleTraverser
import org.codehaus.groovy.runtime.InvokerHelper

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class SugarTestHelper {

    /**
     * Clear the metaclass registry to "turn-off" sugar.
     */
    public static void clearRegistry(final GraphProvider graphProvider) {
        final Set<Class> implementationsToClear = new HashSet<>(GraphProvider.CORE_IMPLEMENTATIONS)
        implementationsToClear.addAll(graphProvider.getImplementations());

        MetaRegistryUtil.clearRegistry(implementationsToClear)
    }
}
