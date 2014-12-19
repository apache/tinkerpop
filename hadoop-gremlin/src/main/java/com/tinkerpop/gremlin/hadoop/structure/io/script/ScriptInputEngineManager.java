package com.tinkerpop.gremlin.hadoop.structure.io.script;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 */
class ScriptInputEngineManager {

    public final static String DEFAULT_SCRIPT_ENGINE = "gremlin-groovy";

    private final static ScriptEngineManager SCRIPT_ENGINE_MANAGER = new ScriptEngineManager();
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
