/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.structure.io.util;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class IoRegistryHelper {

    private IoRegistryHelper() {

    }

    public static List<IoRegistry> createRegistries(final List<Object> registryNamesClassesOrInstances) {
        if (registryNamesClassesOrInstances.isEmpty()) return Collections.emptyList();

        final List<IoRegistry> registries = new ArrayList<>();
        for (final Object object : registryNamesClassesOrInstances) {
            if (object instanceof IoRegistry)
                registries.add((IoRegistry) object);
            else if (object instanceof String || object instanceof Class) {
                try {
                    final Class<?> clazz = object instanceof String ? Class.forName((String) object) : (Class) object;
                    Method instanceMethod = null;
                    try {
                        instanceMethod = clazz.getDeclaredMethod("instance"); // try for getInstance() ??
                    } catch (final NoSuchMethodException e) {
                        try {
                            instanceMethod = clazz.getDeclaredMethod("getInstance"); // try for getInstance() ??
                        } catch (final NoSuchMethodException e2) {
                            // no instance() or getInstance() methods
                        }
                    }
                    if (null != instanceMethod && IoRegistry.class.isAssignableFrom(instanceMethod.getReturnType()))
                        registries.add((IoRegistry) instanceMethod.invoke(null));
                    else
                        registries.add((IoRegistry) clazz.newInstance()); // no instance() or getInstance() methods, try instantiate class
                } catch (final Exception e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            } else {
                throw new IllegalArgumentException("The provided registry object can not be resolved to an instance: " + object);
            }
        }
        return registries;
    }

    public static List<IoRegistry> createRegistries(final Configuration configuration) {
        if (configuration.containsKey(IoRegistry.IO_REGISTRY)) {
            final Object property = configuration.getProperty(IoRegistry.IO_REGISTRY);
            if (property instanceof IoRegistry)
                return Collections.singletonList((IoRegistry) property);
            else if (property instanceof List)
                return createRegistries((List) property);
            else if (property instanceof String)
                return createRegistries(Arrays.asList(((String) property).split(",")));
            else
                throw new IllegalArgumentException("The provided registry object can not be resolved to an instance: " + property);
        } else
            return Collections.emptyList();
    }
}
