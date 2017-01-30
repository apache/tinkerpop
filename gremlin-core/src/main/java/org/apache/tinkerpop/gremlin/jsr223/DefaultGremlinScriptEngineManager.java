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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
 * This class is based quite heavily on the workings of the {@code ScriptEngineManager} supplied in the
 * {@code javax.script} packages, but adds some additional features that are specific to Gremlin and TinkerPop.
 * Unfortunately, it's not easily possible to extend {@code ScriptEngineManager} directly as there certain behaviors
 * don't appear to be be straightforward to implement and member variables are all private. It is important to note
 * that this class is designed to provide support for "Gremlin-enabled" {@code ScriptEngine} instances (i.e. those
 * that extend from {@link GremlinScriptEngine}) and is not meant to manage just any {@code ScriptEngine} instance
 * that may be on the path.
 * <p/>
 * As this is a "Gremlin" {@code ScriptEngine}, certain common imports are automatically applied when a
 * {@link GremlinScriptEngine} is instantiated via the {@link GremlinScriptEngineFactory}.. Initial imports from
 * gremlin-core come from the {@link CoreImports}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultGremlinScriptEngineManager implements GremlinScriptEngineManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultGremlinScriptEngineManager.class);

    /**
     * Set of script engine factories discovered.
     */
    private final HashSet<GremlinScriptEngineFactory> engineSpis = new HashSet<>();

    /**
     * Map of engine name to script engine factory.
     */
    private final HashMap<String, GremlinScriptEngineFactory> nameAssociations = new HashMap<>();

    /**
     * Map of script file extension to script engine factory.
     */
    private final HashMap<String, GremlinScriptEngineFactory> extensionAssociations = new HashMap<>();

    /**
     *  Map of script script MIME type to script engine factory.
     */
    private final HashMap<String, GremlinScriptEngineFactory> mimeTypeAssociations = new HashMap<>();

    /**
     * Global bindings associated with script engines created by this manager.
     */
    private Bindings globalScope = new ConcurrentBindings();

    /**
     * List of extensions for the {@link GremlinScriptEngineManager} which will be used to supply
     * {@link Customizer} instances to {@link GremlinScriptEngineFactory} that are instantiated.
     */
    private List<GremlinPlugin> plugins = new ArrayList<>();

    /**
     * The effect of calling this constructor is the same as calling
     * {@code DefaultGremlinScriptEngineManager(Thread.currentThread().getContextClassLoader())}.
     */
    public DefaultGremlinScriptEngineManager() {
        final ClassLoader ctxtLoader = Thread.currentThread().getContextClassLoader();
        initEngines(ctxtLoader);
    }

    /**
     * This constructor loads the implementations of {@link GremlinScriptEngineFactory} visible to the given
     * {@code ClassLoader} using the {@code ServiceLoader} mechanism. If loader is <code>null</code>, the script
     * engine factories that are bundled with the platform and that are in the usual extension directories
     * (installed extensions) are loaded.
     */
    public DefaultGremlinScriptEngineManager(final ClassLoader loader) {
        initEngines(loader);
    }

    @Override
    public List<Customizer> getCustomizers(final String scriptEngineName) {
        final List<Customizer> pluginCustomizers = plugins.stream().flatMap(plugin -> {
            final Optional<Customizer[]> customizers = plugin.getCustomizers(scriptEngineName);
            return Stream.of(customizers.orElse(new Customizer[0]));
        }).collect(Collectors.toList());

        return pluginCustomizers;
    }

    @Override
    public void addPlugin(final GremlinPlugin plugin) {
        // TODO: should modules be a set based on "name" to ensure uniqueness? not sure what bad stuff can happen with dupes
        if (plugin != null) plugins.add(plugin);
    }

    /**
     * Stores the specified {@code Bindings} as a global for all {@link GremlinScriptEngine} objects created by it.
     * If the bindings are to be updated by multiple threads it is recommended that a {@link ConcurrentBindings}
     * instance is supplied.
     *
     * @throws IllegalArgumentException if bindings is null.
     */
    @Override
    public synchronized void setBindings(final Bindings bindings) {
        if (null == bindings) throw new IllegalArgumentException("Global scope cannot be null.");
        globalScope = bindings;
    }

    /**
     * Gets the bindings of the {@code Bindings} in global scope.
     */
    @Override
    public Bindings getBindings() {
        return globalScope;
    }

    /**
     * Sets the specified key/value pair in the global scope. The key may not be null or empty.
     *
     * @throws IllegalArgumentException if key is null or empty.
     */
    @Override
    public void put(final String key, final Object value) {
        if (null == key) throw new IllegalArgumentException("key may not be null");
        if (key.isEmpty()) throw new IllegalArgumentException("key may not be empty");
        globalScope.put(key, value);
    }

    /**
     * Gets the value for the specified key in the global scope.
     */
    @Override
    public Object get(final String key) {
        return globalScope.get(key);
    }

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
    @Override
    public GremlinScriptEngine getEngineByName(final String shortName) {
        if (null == shortName) throw new NullPointerException();
        //look for registered name first
        Object obj;
        if (null != (obj = nameAssociations.get(shortName))) {
            final GremlinScriptEngineFactory spi = (GremlinScriptEngineFactory) obj;
            try {
                return createGremlinScriptEngine(spi);
            } catch (Exception exp) {
                logger.error(String.format("Could not create GremlinScriptEngine for %s", shortName), exp);
            }
        }

        for (GremlinScriptEngineFactory spi : engineSpis) {
            List<String> names = null;
            try {
                names = spi.getNames();
            } catch (Exception exp) {
                logger.error("Could not get GremlinScriptEngine names", exp);
            }

            if (names != null) {
                for (String name : names) {
                    if (shortName.equals(name)) {
                        try {
                            return createGremlinScriptEngine(spi);
                        } catch (Exception exp) {
                            logger.error(String.format("Could not create GremlinScriptEngine for %s", shortName), exp);
                        }
                    }
                }
            }
        }

        return null;
    }

    /**
     * Look up and create a {@link GremlinScriptEngine} for a given extension.  The algorithm
     * used by {@link #getEngineByName(String)} is used except that the search starts by looking for a
     * {@link GremlinScriptEngineFactory} registered to handle the given extension using
     * {@link #registerEngineExtension(String, GremlinScriptEngineFactory)}.
     *
     * @return The engine to handle scripts with this extension.  Returns {@code null} if not found.
     * @throws NullPointerException if extension is {@code null}.
     */
    @Override
    public GremlinScriptEngine getEngineByExtension(final String extension) {
        if (null == extension) throw new NullPointerException();
        //look for registered extension first
        Object obj;
        if (null != (obj = extensionAssociations.get(extension))) {
            final GremlinScriptEngineFactory spi = (GremlinScriptEngineFactory) obj;
            try {
                return createGremlinScriptEngine(spi);
            } catch (Exception exp) {
                logger.error(String.format("Could not create GremlinScriptEngine for %s", extension), exp);
            }
        }

        for (GremlinScriptEngineFactory spi : engineSpis) {
            List<String> exts = null;
            try {
                exts = spi.getExtensions();
            } catch (Exception exp) {
                logger.error("Could not get GremlinScriptEngine extensions", exp);
            }
            if (exts == null) continue;
            for (String ext : exts) {
                if (extension.equals(ext)) {
                    try {
                        return createGremlinScriptEngine(spi);
                    } catch (Exception exp) {
                        logger.error(String.format("Could not create GremlinScriptEngine for %s", extension), exp);
                    }
                }
            }
        }
        return null;
    }

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
    @Override
    public GremlinScriptEngine getEngineByMimeType(final String mimeType) {
        if (null == mimeType) throw new NullPointerException();
        //look for registered types first
        Object obj;
        if (null != (obj = mimeTypeAssociations.get(mimeType))) {
            final GremlinScriptEngineFactory spi = (GremlinScriptEngineFactory) obj;
            try {
                return createGremlinScriptEngine(spi);
            } catch (Exception exp) {
                logger.error(String.format("Could not create GremlinScriptEngine for %s", mimeType), exp);
            }
        }

        for (GremlinScriptEngineFactory spi : engineSpis) {
            List<String> types = null;
            try {
                types = spi.getMimeTypes();
            } catch (Exception exp) {
                logger.error("Could not get GremlinScriptEngine mimetypes", exp);
            }
            if (types == null) continue;
            for (String type : types) {
                if (mimeType.equals(type)) {
                    try {
                        return createGremlinScriptEngine(spi);
                    } catch (Exception exp) {
                        logger.error(String.format("Could not create GremlinScriptEngine for %s", mimeType), exp);
                    }
                }
            }
        }
        return null;
    }

    /**
     * Returns a list whose elements are instances of all the {@link GremlinScriptEngineFactory} classes
     * found by the discovery mechanism.
     *
     * @return List of all discovered {@link GremlinScriptEngineFactory} objects.
     */
    @Override
    public List<GremlinScriptEngineFactory> getEngineFactories() {
        final List<GremlinScriptEngineFactory> res = new ArrayList<>(engineSpis.size());
        res.addAll(engineSpis.stream().collect(Collectors.toList()));
        return Collections.unmodifiableList(res);
    }

    /**
     * Registers a {@link GremlinScriptEngineFactory} to handle a language name.  Overrides any such association found
     * using the discovery mechanism.
     *
     * @param name The name to be associated with the {@link GremlinScriptEngineFactory}
     * @param factory The class to associate with the given name.
     * @throws NullPointerException if any of the parameters is null.
     */
    @Override
    public void registerEngineName(final String name, final GremlinScriptEngineFactory factory) {
        if (null == name || null == factory) throw new NullPointerException();
        nameAssociations.put(name, factory);
    }

    /**
     * Registers a {@link GremlinScriptEngineFactory} to handle a mime type. Overrides any such association found using
     * the discovery mechanism.
     *
     * @param type The mime type  to be associated with the {@link GremlinScriptEngineFactory}.
     * @param factory The class to associate with the given mime type.
     * @throws NullPointerException if any of the parameters is null.
     */
    @Override
    public void registerEngineMimeType(final String type, final GremlinScriptEngineFactory factory) {
        if (null == type || null == factory) throw new NullPointerException();
        mimeTypeAssociations.put(type, factory);
    }

    /**
     * Registers a {@link GremlinScriptEngineFactory} to handle an extension. Overrides any such association found
     * using the discovery mechanism.
     *
     * @param extension The extension type to be associated with the {@link GremlinScriptEngineFactory}
     * @param factory The class to associate with the given extension.
     * @throws NullPointerException if any of the parameters is null.
     */
    @Override
    public void registerEngineExtension(final String extension, final GremlinScriptEngineFactory factory) {
        if (null == extension || null == factory) throw new NullPointerException();
        extensionAssociations.put(extension, factory);
    }

    private ServiceLoader<GremlinScriptEngineFactory> getServiceLoader(final ClassLoader loader) {
        if (loader != null) {
            return ServiceLoader.load(GremlinScriptEngineFactory.class, loader);
        } else {
            return ServiceLoader.loadInstalled(GremlinScriptEngineFactory.class);
        }
    }

    private void initEngines(final ClassLoader loader) {
        Iterator<GremlinScriptEngineFactory> itty;
        try {
            final ServiceLoader<GremlinScriptEngineFactory> sl = AccessController.doPrivileged(
                    (PrivilegedAction<ServiceLoader<GremlinScriptEngineFactory>>) () -> getServiceLoader(loader));
            itty = sl.iterator();
        } catch (ServiceConfigurationError err) {
            logger.error("Can't find GremlinScriptEngineFactory providers: " + err.getMessage(), err);

            // do not throw any exception here. user may want to manager their own factories using this manager
            // by explicit registration (by registerXXX) methods.
            return;
        }

        try {
            while (itty.hasNext()) {
                try {
                    final GremlinScriptEngineFactory factory = itty.next();
                    factory.setCustomizerManager(this);
                    engineSpis.add(factory);
                } catch (ServiceConfigurationError err) {
                    logger.error("GremlinScriptEngineManager providers.next(): " + err.getMessage(), err);
                }
            }
        } catch (ServiceConfigurationError err) {
            logger.error("GremlinScriptEngineManager providers.hasNext(): " + err.getMessage(), err);
            // do not throw any exception here. user may want to manage their own factories using this manager
            // by explicit registration (by registerXXX) methods.
        }
    }

    private GremlinScriptEngine createGremlinScriptEngine(final GremlinScriptEngineFactory spi) {
        final GremlinScriptEngine engine = spi.getScriptEngine();

        // merge in bindings that are marked with global scope. these get applied to all GremlinScriptEngine instances
        getCustomizers(spi.getEngineName()).stream()
                .filter(p -> p instanceof BindingsCustomizer)
                .map(p -> ((BindingsCustomizer) p))
                .filter(bc -> bc.getScope() == ScriptContext.GLOBAL_SCOPE)
                .flatMap(bc -> bc.getBindings().entrySet().stream())
                .forEach(kv -> {
                    if (globalScope.containsKey(kv.getKey())) {
                        logger.warn("Overriding the global binding [{}] - was [{}] and is now [{}]",
                                kv.getKey(), globalScope.get(kv.getKey()), kv.getValue());
                    }

                    globalScope.put(kv.getKey(), kv.getValue());
                });
        engine.setBindings(getBindings(), ScriptContext.GLOBAL_SCOPE);

        // merge in bindings that are marked with engine scope. there typically won't be any of these but it's just
        // here for completeness. bindings will typically apply with global scope only as engine scope will generally
        // be overridden at the time of eval() with the bindings that are supplied to it
        getCustomizers(spi.getEngineName()).stream()
                .filter(p -> p instanceof BindingsCustomizer)
                .map(p -> ((BindingsCustomizer) p))
                .filter(bc -> bc.getScope() == ScriptContext.ENGINE_SCOPE)
                .forEach(bc -> engine.getBindings(ScriptContext.ENGINE_SCOPE).putAll(bc.getBindings()));

        final List<ScriptCustomizer> scriptCustomizers = getCustomizers(spi.getEngineName()).stream()
                .filter(p -> p instanceof ScriptCustomizer)
                .map(p -> ((ScriptCustomizer) p))
                .collect(Collectors.toList());

        // since the bindings aren't added until after the ScriptEngine is constructed, running init scripts that
        // require bindings creates a problem. as a result, init scripts are applied here
        scriptCustomizers.stream().flatMap(sc -> sc.getScripts().stream()).
                map(l -> String.join(System.lineSeparator(), l)).forEach(initScript -> {
            try {
                final Object initializedBindings = engine.eval(initScript);
                if (initializedBindings != null && initializedBindings instanceof Map)
                    ((Map<String,Object>) initializedBindings).forEach((k,v) -> put(k,v));
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        });

        return engine;
    }
}
