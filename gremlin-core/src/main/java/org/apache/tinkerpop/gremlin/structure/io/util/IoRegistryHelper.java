/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.structure.io.util;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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
                    // a String naming the class is loaded without being initialized, since this value can originate
                    // from a traversal (io() writes its with() options into the graph configuration on the OLAP path)
                    // and the name must be proven to be an IoRegistry before anything the class declares runs. a Class
                    // was resolved by whoever supplied it, so there is nothing left to defer there
                    final Class<?> clazz = object instanceof String
                            ? Class.forName((String) object, false, IoRegistryHelper.class.getClassLoader())
                            : (Class) object;

                    // checked ahead of the instance() lookup and the constructor below, so that neither a static
                    // method nor a constructor on some other class is invoked for its side effects
                    if (!IoRegistry.class.isAssignableFrom(clazz))
                        throw new IllegalStateException("The provided registry object can not be resolved to an instance: " + object);

                    Method instanceMethod = null;
                    try {
                        instanceMethod = clazz.getDeclaredMethod("instance"); // try for getInstance() ??
                    } catch (final NoSuchMethodException e) {
                        try {
                            // even though use of "getInstance" is no longer a thing in tinkerpop as of 3.3.0, perhaps
                            // others are using this style of naming. Doesn't seem to harm anything to continue to
                            // check for that method.
                            instanceMethod = clazz.getDeclaredMethod("getInstance"); // try for getInstance() ??
                        } catch (final NoSuchMethodException e2) {
                            // no instance() or getInstance() methods
                        }
                    }
                    if (null != instanceMethod && Modifier.isStatic(instanceMethod.getModifiers())
                            && IoRegistry.class.isAssignableFrom(instanceMethod.getReturnType()))
                        registries.add((IoRegistry) instanceMethod.invoke(null));
                    else
                        registries.add((IoRegistry) clazz.newInstance()); // no instance() or getInstance() methods, try instantiate class
                } catch (final IllegalStateException ise) {
                    throw ise;
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
