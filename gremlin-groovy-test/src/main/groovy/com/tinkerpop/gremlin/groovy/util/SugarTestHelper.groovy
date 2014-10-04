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
        def metaRegistry = InvokerHelper.getMetaRegistry()

        // this call returns interfaces and removes meta clases from there.  not sure why it doesn't return
        // concrete classes that are in the registry, but such is the nature of groovy
        def metaClassesToRemove = metaRegistry.iterator()
        metaClassesToRemove.collect{(Class) it.theClass}.each{ metaRegistry.removeMetaClass(it) }

        // since we don't get concrete classes those must come from the GraphProvider.
        graphProvider.getImplementations().each{ metaRegistry.removeMetaClass(it) }
    }
}
