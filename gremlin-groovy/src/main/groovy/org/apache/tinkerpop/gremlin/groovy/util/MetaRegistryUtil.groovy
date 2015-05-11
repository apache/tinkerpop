/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.groovy.util

/**
 * Helper functions for working with the Groovy {@code MetaRegistry}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class MetaRegistryUtil {

    /**
     * Clears the {@code MetaRegistry} of any meta-programming registration.  In other words, if dynamic methods
     * were added to a class, then this method will clear those methods from the supplied classes.
     */
    public static void clearRegistry(final Set<Class> toClear) {
        def metaRegistry = GroovySystem.metaClassRegistry

        // this call returns interfaces and removes meta clases from there.  not sure why it doesn't return
        // concrete classes that are in the registry, but such is the nature of groovy
        def metaClassesToRemove = metaRegistry.iterator()
        metaClassesToRemove.collect { (Class) it.theClass }.each { metaRegistry.removeMetaClass(it) }

        // since we don't get concrete classes those must come from the GraphProvider.
        toClear.each { metaRegistry.removeMetaClass(it) }
    }
}
