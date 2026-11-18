/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.StringJoiner;

/**
 * An SPI, discovered with {@link ServiceLoader}, through which a {@link Graph} implementation registers itself as
 * TinkerPop's default in-memory graph. {@code gremlin-core} has no compile-time dependency on any concrete
 * {@link Graph} implementation, so this is how it opens one when it needs a graph to deserialize a remote
 * {@code Graph} result into, or to use as the default {@code subgraph()} side-effect, without naming that
 * implementation directly. {@code tinkergraph-gremlin} provides this service for {@code TinkerGraph}.
 * <p/>
 * Implementations need a public no-argument constructor and must be registered in
 * {@code META-INF/services/org.apache.tinkerpop.gremlin.structure.util.DefaultGraphProvider} for the class path
 * and with a {@code provides} clause in their module descriptor for the module path. Only one distinct
 * implementation may be present. Repeated registrations of the same implementation class count as one provider.
 */
public interface DefaultGraphProvider {

    /**
     * The message used when no {@link DefaultGraphProvider} can be found, naming the ways to fix it.
     */
    String NO_PROVIDER_MESSAGE = "No DefaultGraphProvider was found to supply the Graph implementation needed to " +
            "deserialize a Graph result or to hold a subgraph() side-effect - add tinkergraph-gremlin as a " +
            "dependency. On the module path, also make sure the org.apache.tinkerpop.gremlin.tinkergraph module is " +
            "present: it binds automatically through its DefaultGraphProvider service once it is on the module " +
            "path, or it can be added explicitly with 'requires org.apache.tinkerpop.gremlin.tinkergraph' or " +
            "'--add-modules org.apache.tinkerpop.gremlin.tinkergraph'.";

    /**
     * Opens a new, empty instance of the provided {@link Graph} implementation.
     *
     * @param configuration configuration for the graph; an implementation should tolerate an empty configuration
     *                      and apply its own defaults
     */
    Graph open(final Configuration configuration);

    /**
     * Finds the {@link DefaultGraphProvider}, if any, on the class or module path. The class loader of
     * {@code gremlin-core} is searched first and the thread context class loader second, so a provider that
     * {@code gremlin-core} can use is preferred even when a child-first context class loader carries its own copies
     * of {@code gremlin-core} and {@code tinkergraph-gremlin}. A provider that fails to load, for example because it
     * implements another copy of this interface, is skipped rather than failing the lookup. Repeated registrations
     * of the same implementation class count as one provider.
     *
     * @throws IllegalStateException if more than one distinct provider implementation is found
     */
    static Optional<DefaultGraphProvider> find() {
        final ClassLoader own = DefaultGraphProvider.class.getClassLoader();
        final Optional<DefaultGraphProvider> provider = findWith(own);
        final ClassLoader context = Thread.currentThread().getContextClassLoader();
        return provider.isPresent() || context == own ? provider : findWith(context);
    }

    private static Optional<DefaultGraphProvider> findWith(final ClassLoader classLoader) {
        final Iterator<DefaultGraphProvider> providerIterator =
                ServiceLoader.load(DefaultGraphProvider.class, classLoader).iterator();
        final Map<Class<?>, DefaultGraphProvider> providers = new LinkedHashMap<>();
        while (true) {
            try {
                if (!providerIterator.hasNext()) return selectProvider(providers);
            } catch (final ServiceConfigurationError ignored) {
                // the services files can't be read, so nothing more can be found through this class loader
                return selectProvider(providers);
            }

            try {
                final DefaultGraphProvider provider = providerIterator.next();
                providers.putIfAbsent(provider.getClass(), provider);
            } catch (final ServiceConfigurationError ignored) {
                // a provider that can't be loaded or isn't a subtype of this interface - try the next one
            }
        }
    }

    private static Optional<DefaultGraphProvider> selectProvider(
            final Map<Class<?>, DefaultGraphProvider> providers) {
        if (providers.size() > 1) {
            final StringJoiner names = new StringJoiner(", ");
            providers.keySet().forEach(providerClass -> names.add(providerClass.getName()));
            throw new IllegalStateException("Multiple DefaultGraphProvider implementations were found: " + names +
                    ". Only one may be present on the class path or module path.");
        }

        return providers.values().stream().findFirst();
    }
}
