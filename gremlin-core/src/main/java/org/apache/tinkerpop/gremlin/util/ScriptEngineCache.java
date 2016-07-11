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
package org.apache.tinkerpop.gremlin.util;

import org.apache.tinkerpop.gremlin.jsr223.DefaultGremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.jsr223.SingleGremlinScriptEngineManager;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @deprecated As of release 3.3.0, replaced by {@link SingleGremlinScriptEngineManager}.
 */
@Deprecated
public final class ScriptEngineCache {

    private ScriptEngineCache() {}

    public final static String DEFAULT_SCRIPT_ENGINE = "gremlin-groovy";

    private final static GremlinScriptEngineManager SCRIPT_ENGINE_MANAGER = new DefaultGremlinScriptEngineManager();
    private final static Map<String, ScriptEngine> CACHED_ENGINES = new ConcurrentHashMap<>();

    public static ScriptEngine get(final String engineName) {
        return CACHED_ENGINES.compute(engineName, (key, engine) -> {
            if (null == engine) {
                engine = SCRIPT_ENGINE_MANAGER.getEngineByName(engineName);
                if (null == engine) {
                    throw new IllegalArgumentException("There is no script engine with provided name: " + engineName);
                }
            }
            return engine;
        });
    }
}
