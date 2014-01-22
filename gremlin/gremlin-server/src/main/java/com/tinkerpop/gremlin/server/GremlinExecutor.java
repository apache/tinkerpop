package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.server.util.LocalExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Execute Gremlin scripts against a {@code ScriptEngine} instance.  The {@code ScriptEngine} maybe be a shared
 * {@code ScriptEngine} in the case of a sessionless request or a standalone {@code ScriptEngine} bound to a session
 * that always executes within the same thread for every request.
 * <p>
 * The shared {@code ScriptEngine} for sessionless requests can not be dynamically re-initialized as doing so
 * essentially equates to restarting the server.  It's easier to just do that. The sessioned {@code ScriptEngine} is
 * initialized with settings once per session and can be reset by the session itself.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinExecutor {
    private static final Logger logger = LoggerFactory.getLogger(GremlinExecutor.class);

    /**
     * {@link ScriptEngines} instance to evaluate Gremlin script requests.
     */
    private ScriptEngines sharedScriptEngines;

    /**
     * True if initialized and false otherwise.
     */
    private boolean initialized = false;

    private Settings settings = null;

    /**
     * Evaluate the {@link RequestMessage} within a {@code ScriptEngine} instance.
     *
     * @param message the current message
     * @param graphs the list of {@link com.tinkerpop.blueprints.Graph} instances configured for the server
     * @return the result from the evaluation
     */
    public Object eval(final RequestMessage message, final Graphs graphs)
            throws ScriptException, InterruptedException, ExecutionException, TimeoutException {

        final Bindings bindings = new SimpleBindings();
        bindings.putAll(extractBindingsFromMessage(message));

        final String language = message.<String>optionalArgs(Tokens.ARGS_LANGUAGE).orElse("gremlin-groovy");
        bindings.putAll(graphs.getGraphs());

        final ExecutorService executorService = LocalExecutorService.getLocal();
        try {
            // do a safety cleanup of previous transaction...if any
            executorService.submit(graphs::rollbackAll).get();
            final Future<Object> future = executorService.submit((Callable<Object>) () ->
                    sharedScriptEngines.eval(message.<String>optionalArgs(Tokens.ARGS_GREMLIN).get(), bindings, language));
            final Object o = future.get(settings.scriptEvaluationTimeout, TimeUnit.MILLISECONDS);
            executorService.submit(graphs::commitAll).get();
            return o;
        } catch (Exception ex) {
            // todo: gotta work on error handling for failed scripts..........
            executorService.submit(graphs::rollbackAll).get();
            throw ex;
        }
    }

    /**
     * Initialize the {@link GremlinExecutor} given the provided {@link Settings}.  This method is idempotent.
     *
     * @param settings the Gremlin Server configuration
     */
    public synchronized void init(final Settings settings) {
        if (!initialized) {
            if (logger.isDebugEnabled()) logger.debug("Initializing GremlinExecutor");
            sharedScriptEngines = createScriptEngine(settings);

            this.settings = settings;
            initialized = true;
        }
    }

    private synchronized static ScriptEngines createScriptEngine(final Settings settings) {
        final ScriptEngines scriptEngines = new ScriptEngines();
        for (Map.Entry<String,Settings.ScriptEngineSettings> config: settings.scriptEngines.entrySet()) {
            scriptEngines.reload(config.getKey(), new HashSet<>(config.getValue().imports),
                    new HashSet<>(config.getValue().staticImports));
        }

        settings.use.forEach(u-> {
            if (u.size() != 3)
                logger.warn("Could not resolve dependencies for [{}].  Each entry for the 'use' configuration must include [groupId, artifactId, version]", u);
            else {
                logger.info("Getting dependencies for [{}]", u);
                scriptEngines.use(u.get(0), u.get(1), u.get(2));
            }
        });

        return scriptEngines;
    }

    public ScriptEngines getSharedScriptEngines() {
        return this.sharedScriptEngines;
    }

    /**
     * Determines whether or not the {@link GremlinExecutor} was initialized with the appropriate settings or not.
     */
    public boolean isInitialized() {
        return initialized;
    }

    private static Map<String,Object> extractBindingsFromMessage(final RequestMessage msg) {
        final Map<String, Object> m = new HashMap<>();
        final Optional<Map<String,Object>> bindingsInMessage = msg.optionalArgs(Tokens.ARGS_BINDINGS);
        return bindingsInMessage.orElse(m);
    }
}