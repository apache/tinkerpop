package com.tinkerpop.gremlin.groovy.util

import org.codehaus.groovy.runtime.InvokerHelper

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class MetaRegistryUtil {
    public static void clearRegistry(final Set<Class> toClear) {
        def metaRegistry = InvokerHelper.getMetaRegistry()

        // this call returns interfaces and removes meta clases from there.  not sure why it doesn't return
        // concrete classes that are in the registry, but such is the nature of groovy
        def metaClassesToRemove = metaRegistry.iterator()
        metaClassesToRemove.collect{(Class) it.theClass}.each{ metaRegistry.removeMetaClass(it) }

        // since we don't get concrete classes those must come from the GraphProvider.
        toClear.each{ metaRegistry.removeMetaClass(it) }
    }
}
