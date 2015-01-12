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
     * Implementations from gremlin-core that need to be part of the clear process.  This does not exempt
     * vendors from having to register their extensions to any of these classes, but does prevent them from
     * having to register them in addition to their own.
     */
    private static final Set<Class> CORE_IMPLEMENTATIONS = new HashSet<Class>() {{
        add(AnonymousGraphTraversal.Tokens.class);
        add(DefaultGraphTraversal.class);
        add(SimpleTraverser.class);
        add(PathTraverser.class);
    }};

    /**
     * Clear the metaclass registry to "turn-off" sugar.
     */
    public static void clearRegistry(final GraphProvider graphProvider) {
        final Set<Class> implementationsToClear = new HashSet<>(CORE_IMPLEMENTATIONS)
        implementationsToClear.addAll(graphProvider.getImplementations());

        MetaRegistryUtil.clearRegistry(implementationsToClear)
    }
}
