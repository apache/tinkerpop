package com.tinkerpop.gremlin.groovy.engine;

import com.tinkerpop.gremlin.groovy.DefaultImportCustomizerProvider;
import com.tinkerpop.gremlin.groovy.SecurityCustomizerProvider;
import com.tinkerpop.gremlin.groovy.jsr223.DependencyManager;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineFactory;
import com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import org.kohsuke.groovy.sandbox.GroovyInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Holds a batch of the configured {@code ScriptEngine} objects for the server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ScriptEngines implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ScriptEngines.class);

    /**
     * {@code ScriptEngine} objects configured for the server keyed on the language name.
     */
    private final Map<String, ScriptEngine> scriptEngines = new ConcurrentHashMap<>();

    private final AtomicInteger evaluationCount = new AtomicInteger(0);
    private volatile boolean controlOperationExecuting = false;

    private static final GremlinGroovyScriptEngineFactory gremlinGroovyScriptEngineFactory = new GremlinGroovyScriptEngineFactory();
    private final Consumer<ScriptEngines> initializer;

    public ScriptEngines(final Consumer<ScriptEngines> initializer) {
        this.initializer = initializer;
        this.initializer.accept(this);
    }

    /**
     * Evaluate a script with {@code Bindings} for a particular language.
     */
    public Object eval(final String script, final Bindings bindings, final String language) throws ScriptException {
        if (!scriptEngines.containsKey(language))
            throw new IllegalArgumentException(String.format("Language [%s] not supported", language));

        try {
            awaitControlOp();
            evaluationCount.incrementAndGet();
            return scriptEngines.get(language).eval(script, bindings);
        } finally {
            evaluationCount.decrementAndGet();
        }
    }

    /**
     * Evaluate a script with {@code Bindings} for a particular language.
     */
    public Object eval(final Reader reader, final Bindings bindings, final String language)
            throws ScriptException {
        if (!scriptEngines.containsKey(language))
            throw new IllegalArgumentException("Language [%s] not supported");

        try {
            evaluationCount.incrementAndGet();
            return scriptEngines.get(language).eval(reader, bindings);
        } finally {
            evaluationCount.decrementAndGet();
        }
    }

    /**
     * Reload a {@code ScriptEngine} with fresh imports.  Waits for any existing script evaluations to complete but
     * then blocks other operations until complete.
     */
    public void reload(final String language, final Set<String> imports, final Set<String> staticImports, final Map<String, Object> config) {
        signalControlOp();

        try {
            if (scriptEngines.containsKey(language))
                scriptEngines.remove(language);

            final ScriptEngine scriptEngine = createScriptEngine(language, imports, staticImports, config)
                    .orElseThrow(() -> new IllegalArgumentException("Language [%s] not supported"));
            scriptEngines.put(language, scriptEngine);

            logger.info("Loaded {} ScriptEngine", language);
        } finally {
            controlOperationExecuting = false;
        }
    }

    /**
     * Perform append to the existing import list for all {@code ScriptEngine} instances that implement the
     * {@link DependencyManager} interface.  Waits for any existing script evaluations to complete but
     * then blocks other operations until complete.
     */
    public void addImports(final Set<String> imports) {
        signalControlOp();

        try {
            getDependencyManagers().forEach(dm -> dm.addImports(imports));
        } finally {
            controlOperationExecuting = false;
        }
    }

    /**
     * Pull in dependencies given some Maven coordinates.  Cycle through each {@code ScriptEngine} and determine if it
     * implements {@link DependencyManager}.  For those that do call the @{link DependencyManager#use} method to fire
     * it up.  Waits for any existing script evaluations to complete but then blocks other operations until complete.
     */
    public List<GremlinPlugin> use(final String group, final String artifact, final String version) {
        signalControlOp();

        final List<GremlinPlugin> pluginsToLoad = new ArrayList<>();
        try {
            getDependencyManagers().forEach(dm -> {
                try {
                    pluginsToLoad.addAll(dm.use(group, artifact, version));
                } catch (Exception ex) {
                    logger.warn("Could not get dependency for [{}, {}, {}] - {}", group, artifact, version, ex.getMessage());
                }
            });
        } finally {
            controlOperationExecuting = false;
        }

        return pluginsToLoad;
    }

    public void loadPlugins(final List<GremlinPlugin> plugins) {
        signalControlOp();

        getDependencyManagers().forEach(dm -> {
            try {
                dm.loadPlugins(plugins);
            } catch (IllegalEnvironmentException iee) {
                logger.warn("Some plugins may not have been loaded to {} - {}", dm.getClass().getSimpleName(), iee.getMessage());
            } catch (Exception ex) {
                logger.error(String.format("Some plugins may not have been loaded to %s", dm.getClass().getSimpleName()), ex);
            } finally {
                controlOperationExecuting = false;
            }
        });
    }

    @Override
    public void close() throws Exception {
        signalControlOp();

        try {
            scriptEngines.values().stream()
                    .filter(se -> se instanceof Closeable)
                    .map(se -> (Closeable) se).forEach(c -> {
                try {
                    c.close();
                } catch (IOException ignored) {
                }
            });
            scriptEngines.clear();
        } finally {
            controlOperationExecuting = false;
        }
    }

    /**
     * Resets the ScriptEngines and re-initializes them.  Waits for any existing script evaluations to complete but
     * then blocks other operations until complete.
     */
    public void reset() {
        signalControlOp();

        try {
            getDependencyManagers().forEach(DependencyManager::reset);
        } finally {
            controlOperationExecuting = false;
            this.initializer.accept(this);
        }
    }

    /**
     * List dependencies for those {@code ScriptEngine} objects that implement the {@link DependencyManager} interface.
     */
    public Map<String, List<Map>> dependencies() {
        final Map<String, List<Map>> m = new HashMap<>();
        scriptEngines.entrySet().stream()
                .filter(kv -> kv.getValue() instanceof DependencyManager)
                .forEach(kv -> m.put(kv.getKey(), Arrays.asList(((DependencyManager) kv.getValue()).dependencies())));
        return m;
    }

    public Map<String, List<Map>> imports() {
        final Map<String, List<Map>> m = new HashMap<>();
        scriptEngines.entrySet().stream()
                .filter(kv -> kv.getValue() instanceof DependencyManager)
                .forEach(kv -> m.put(kv.getKey(), Arrays.asList(((DependencyManager) kv.getValue()).imports())));
        return m;
    }

    /**
     * Get the set of {@code ScriptEngine} that implement {@link DependencyManager} interface.
     */
    private Set<DependencyManager> getDependencyManagers() {
        return scriptEngines.entrySet().stream()
                .map(Map.Entry::getValue)
                .filter(se -> se instanceof DependencyManager)
                .map(se -> (DependencyManager) se)
                .collect(Collectors.<DependencyManager>toSet());
    }

    private void signalControlOp() {
        awaitControlOp();
        controlOperationExecuting = true;
        awaitEvalOp();
    }

    private void awaitControlOp() {
        while (controlOperationExecuting) {
            try {
                Thread.sleep(5);
            } catch (Exception ignored) {

            }
        }
    }

    private void awaitEvalOp() {
        while (evaluationCount.get() > 0) {
            try {
                Thread.sleep(5);
            } catch (Exception ignored) {

            }
        }
    }

    private static synchronized Optional<ScriptEngine> createScriptEngine(final String language,
                                                                          final Set<String> imports,
                                                                          final Set<String> staticImports,
                                                                          final Map<String, Object> config) {
        // gremlin-groovy gets special initialization for mapper imports and such.  could implement this more
        // generically with the DependencyManager interface, but going to wait to see how other ScriptEngines
        // develop for TinkerPop3 before committing too deeply here to any specific way of doing this.
        if (language.equals(gremlinGroovyScriptEngineFactory.getLanguageName())) {
            final String clazz = (String) config.getOrDefault("sandbox", "");
            SecurityCustomizerProvider securityCustomizerProvider = null;
            if (!clazz.isEmpty()) {
                try {
                    final Class providerClass = Class.forName(clazz);
                    final GroovyInterceptor interceptor = (GroovyInterceptor) providerClass.newInstance();
                    securityCustomizerProvider = new SecurityCustomizerProvider(interceptor);
                } catch (Exception ex) {
                    logger.warn("Could not instantiate GroovyInterceptor implementation [%s] for the SecurityCustomizerProvider.  It will not be applied.", clazz);
                    securityCustomizerProvider = null;
                }
            }

            return Optional.of((ScriptEngine) new GremlinGroovyScriptEngine(
                    new DefaultImportCustomizerProvider(imports, staticImports), securityCustomizerProvider));
        } else {
            final ScriptEngineManager manager = new ScriptEngineManager();
            return Optional.ofNullable(manager.getEngineByName(language));
        }
    }

}
