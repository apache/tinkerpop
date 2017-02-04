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

import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation of the {@link GremlinScriptEngineManager} that caches the instances of the
 * {@link GremlinScriptEngine} instances that are created by it. Note that the cache is relevant to the instance
 * of the {@link CachedGremlinScriptEngineManager} and is not global to the JVM.
 * <p/>
 * {@inheritDoc}
 */
public class CachedGremlinScriptEngineManager extends DefaultGremlinScriptEngineManager {

    private final ConcurrentHashMap<String,GremlinScriptEngine> cache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String,String> extensionToName = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String,String> mimeToName = new ConcurrentHashMap<>();

    /**
     * @see DefaultGremlinScriptEngineManager#DefaultGremlinScriptEngineManager()
     */
    public CachedGremlinScriptEngineManager() {
        super();
    }

    /**
     * @see DefaultGremlinScriptEngineManager#DefaultGremlinScriptEngineManager(ClassLoader loader)
     */
    public CachedGremlinScriptEngineManager(final ClassLoader loader) {
        super(loader);
    }

    /**
     * Gets a {@link GremlinScriptEngine} from cache or creates a new one from the {@link GremlinScriptEngineFactory}.
     * <p/>
     * {@inheritDoc}
     */
    @Override
    public GremlinScriptEngine getEngineByName(final String shortName) {
        final GremlinScriptEngine engine = cache.computeIfAbsent(shortName, super::getEngineByName);
        registerLookUpInfo(engine, shortName);
        return engine;
    }

    /**
     * Gets a {@link GremlinScriptEngine} from cache or creates a new one from the {@link GremlinScriptEngineFactory}.
     * <p/>
     * {@inheritDoc}
     */
    @Override
    public GremlinScriptEngine getEngineByExtension(final String extension) {
        if (!extensionToName.containsKey(extension))  {
            final GremlinScriptEngine engine = super.getEngineByExtension(extension);
            registerLookUpInfo(engine, engine.getFactory().getEngineName());
            return engine;
        }

        return cache.get(extensionToName.get(extension));
    }

    /**
     * Gets a {@link GremlinScriptEngine} from cache or creates a new one from the {@link GremlinScriptEngineFactory}.
     * <p/>
     * {@inheritDoc}
     */
    @Override
    public GremlinScriptEngine getEngineByMimeType(final String mimeType) {
        if (!mimeToName.containsKey(mimeType))  {
            final GremlinScriptEngine engine = super.getEngineByMimeType(mimeType);
            registerLookUpInfo(engine, engine.getFactory().getEngineName());
            return engine;
        }

        return cache.get(mimeToName.get(mimeType));
    }

    private void registerLookUpInfo(final GremlinScriptEngine engine, final String shortName) {
        if (null == engine) throw new IllegalArgumentException(String.format("%s is not an available GremlinScriptEngine", shortName));
        cache.putIfAbsent(shortName, engine);
        engine.getFactory().getExtensions().forEach(ext -> extensionToName.putIfAbsent(ext, shortName));
        engine.getFactory().getMimeTypes().forEach(mime -> mimeToName.putIfAbsent(mime, shortName));
    }
}
