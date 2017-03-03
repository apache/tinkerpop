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
package org.apache.tinkerpop.gremlin.jsr223;

import javax.script.Bindings;
import java.util.List;

/**
 * The {@code ScriptEngineManager} implements a discovery, instantiation and configuration mechanism for
 * {@link GremlinScriptEngine} classes and also maintains a collection of key/value pairs storing state shared by all
 * engines created by it. This class uses the {@code ServiceProvider} mechanism to enumerate all the
 * implementations of <code>GremlinScriptEngineFactory</code>. The <code>ScriptEngineManager</code> provides a method
 * to return a list of all these factories as well as utility methods which look up factories on the basis of language
 * name, file extension and mime type.
 * <p/>
 * The {@code Bindings} of key/value pairs, referred to as the "Global Scope" maintained by the manager is available
 * to all instances of @code ScriptEngine} created by the {@code GremlinScriptEngineManager}. The values
 * in the {@code Bindings} are generally exposed in all scripts.
 * <p/>
 * This interface is based quite heavily on the workings of the {@code ScriptEngineManager} supplied in the
 * {@code javax.script} packages, but adds some additional features that are specific to Gremlin and TinkerPop.
 * Unfortunately, it's not easily possible to extend {@code ScriptEngineManager} directly as there certain behaviors
 * don't appear to be be straightforward to implement and member variables are all private. It is important to note
 * that this interface is designed to provide support for "Gremlin-enabled" {@code ScriptEngine} instances (i.e. those
 * that extend from {@link GremlinScriptEngine}) and is not meant to manage just any {@code ScriptEngine} instance
 * that may be on the path.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GremlinScriptEngineManager {

    /**
     * Stores the specified {@code Bindings} as a global for all {@link GremlinScriptEngine} objects created by it.
     *
     * @throws IllegalArgumentException if bindings is null.
     */
    public void setBindings(final Bindings bindings);

    /**
     * Gets the bindings of the {@code Bindings} in global scope.
     */
    public Bindings getBindings();

    /**
     * Sets the specified key/value pair in the global scope. The key may not be null or empty.
     *
     * @throws IllegalArgumentException if key is null or empty.
     */
    public void put(final String key, final Object value);

    /**
     * Gets the value for the specified key in the global scope.
     */
    public Object get(final String key);

    /**
     * Looks up and creates a {@link GremlinScriptEngine} for a given name. The algorithm first searches for a
     * {@link GremlinScriptEngineFactory} that has been registered as a handler for the specified name using the
     * {@link #registerEngineExtension(String, GremlinScriptEngineFactory)} method. If one is not found, it searches
     * the set of {@code GremlinScriptEngineFactory} instances stored by the constructor for one with the specified
     * name.  If a {@code ScriptEngineFactory} is found by either method, it is used to create instance of
     * {@link GremlinScriptEngine}.
     *
     * @param shortName The short name of the {@link GremlinScriptEngine} implementation returned by the
     * {@link GremlinScriptEngineFactory#getNames} method.
     * @return A {@link GremlinScriptEngine} created by the factory located in the search.  Returns {@code null}
     * if no such factory was found.  The global scope of this manager is applied to the newly created
     * {@link GremlinScriptEngine}
     * @throws NullPointerException if shortName is {@code null}.
     */
    public GremlinScriptEngine getEngineByName(final String shortName);

    /**
     * Look up and create a {@link GremlinScriptEngine} for a given extension.  The algorithm
     * used by {@link #getEngineByName(String)} is used except that the search starts by looking for a
     * {@link GremlinScriptEngineFactory} registered to handle the given extension using
     * {@link #registerEngineExtension(String, GremlinScriptEngineFactory)}.
     *
     * @return The engine to handle scripts with this extension.  Returns {@code null} if not found.
     * @throws NullPointerException if extension is {@code null}.
     */
    public GremlinScriptEngine getEngineByExtension(final String extension);

    /**
     * Look up and create a {@link GremlinScriptEngine} for a given mime type.  The algorithm used by
     * {@link #getEngineByName(String)} is used except that the search starts by looking for a
     * {@link GremlinScriptEngineFactory} registered to handle the given mime type using
     * {@link #registerEngineMimeType(String, GremlinScriptEngineFactory)}.
     *
     * @param mimeType The given mime type
     * @return The engine to handle scripts with this mime type.  Returns {@code null} if not found.
     * @throws NullPointerException if mime-type is {@code null}.
     */
    public GremlinScriptEngine getEngineByMimeType(final String mimeType);

    /**
     * Returns a list whose elements are instances of all the {@link GremlinScriptEngineFactory} classes
     * found by the discovery mechanism.
     *
     * @return List of all discovered {@link GremlinScriptEngineFactory} objects.
     */
    public List<GremlinScriptEngineFactory> getEngineFactories();

    /**
     * Add {@link GremlinPlugin} instances to customize newly created {@link GremlinScriptEngine} instances.
     */
    public void addPlugin(final GremlinPlugin plugin);

    /**
     * Registers a {@link GremlinScriptEngineFactory} to handle a language name.  Overrides any such association found
     * using the discovery mechanism.
     *
     * @param name The name to be associated with the {@link GremlinScriptEngineFactory}
     * @param factory The class to associate with the given name.
     * @throws NullPointerException if any of the parameters is null.
     */
    public void registerEngineName(final String name, final GremlinScriptEngineFactory factory);

    /**
     * Registers a {@link GremlinScriptEngineFactory} to handle a mime type. Overrides any such association found using
     * the discovery mechanism.
     *
     * @param type The mime type  to be associated with the {@link GremlinScriptEngineFactory}.
     * @param factory The class to associate with the given mime type.
     * @throws NullPointerException if any of the parameters is null.
     */
    public void registerEngineMimeType(final String type, final GremlinScriptEngineFactory factory);

    /**
     * Registers a {@link GremlinScriptEngineFactory} to handle an extension. Overrides any such association found
     * using the discovery mechanism.
     *
     * @param extension The extension type to be associated with the {@link GremlinScriptEngineFactory}
     * @param factory The class to associate with the given extension.
     * @throws NullPointerException if any of the parameters is null.
     */
    public void registerEngineExtension(final String extension, final GremlinScriptEngineFactory factory);

    /**
     * Get the list of {@link Customizer} instances filtered by the {@code scriptEngineName}.
     */
    public List<Customizer> getCustomizers(final String scriptEngineName);
}