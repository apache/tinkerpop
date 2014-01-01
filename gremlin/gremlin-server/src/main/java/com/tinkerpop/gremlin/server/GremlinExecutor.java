package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.server.util.LocalExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
     * Used in sessionless mode and centrally configured for imports/scripts.
     */
    private ScriptEngines sharedScriptEngines;

    /**
     * Script engines are evaluated in a per session context where imports/scripts are isolated per session.
     */
    private Map<UUID, GremlinSession> sessionedScriptEngines = new ConcurrentHashMap<>();

    /**
     * True if initialized and false otherwise.
     */
    private boolean initialized = false;

    private Settings settings = null;

    /**
     * Evaluate the {@link RequestMessage} after selecting the appropriate type (in-session or sessionless) of
     * {@code ScriptEngine} instance.
     *
     * @param message the current message
     * @param graphs the list of {@link com.tinkerpop.blueprints.Graph} instances configured for the server
     * @return the result from the evaluation
     */
    public Object eval(final RequestMessage message, final GremlinServer.Graphs graphs)
            throws ScriptException, InterruptedException, ExecutionException, TimeoutException {
        return selectForEval(message, graphs).apply(message);
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

    /**
     * Determines whether or not the {@link GremlinExecutor} was initialized with the appropriate settings or not.
     */
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Determine whether to execute the script by way of a specific session or by the shared {@code ScriptEngine} in
     * a sessionless request.
     */
    private EvalFunctionThatThrows<RequestMessage, Object> selectForEval(final RequestMessage message, final GremlinServer.Graphs graphs) {
        final Bindings bindings = new SimpleBindings();
        bindings.putAll(extractBindingsFromMessage(message));

        final String language = message.<String>optionalArgs(Tokens.ARGS_LANGUAGE).orElse("gremlin-groovy");

        if (message.optionalSessionId().isPresent()) {
            final GremlinSession session = getGremlinSession(message.sessionId);
            if (logger.isDebugEnabled()) logger.debug("Using session {} ScriptEngine to process {}", message.sessionId, message);
            return (RequestMessage m) -> session.eval(m.<String>optionalArgs(Tokens.ARGS_GREMLIN).get(), bindings, language);
        } else {
            // a sessionless request
            if (logger.isDebugEnabled()) logger.debug("Using shared ScriptEngine to process {}", message);
            return (RequestMessage m) -> {
                // put all the preconfigured graphs on the bindings
                bindings.putAll(graphs.getGraphs());

                final ExecutorService executorService = LocalExecutorService.getLocal();
                try {
                    // do a safety cleanup of previous transaction...if any
                    executorService.submit(graphs::rollbackAll).get();
                    final Future<Object> future = executorService.submit((Callable<Object>) () -> sharedScriptEngines.eval(m.<String>optionalArgs(Tokens.ARGS_GREMLIN).get(), bindings, language));
                    final Object o = future.get(settings.scriptEvaluationTimeout, TimeUnit.MILLISECONDS);
                    executorService.submit(graphs::commitAll).get();
                    return o;
                } catch (Exception ex) {
                    // todo: gotta work on error handling for failed scripts..........
                    executorService.submit(graphs::rollbackAll).get();
                    throw ex;
                }
            };
        }
    }

    /**
     * Selects a {@code ScriptEngine}, either shared or session-based, based on the {@link RequestMessage}. If a
     * session identifier is present on the message then return a session-based {@link ScriptEngineOps} otherwise
     * return the shared instance.
     *
     * @param message the current {@link RequestMessage} being processed
     */
    public ScriptEngineOps select(final RequestMessage message) {
        if (message.optionalSessionId().isPresent())
            return getGremlinSession(message.sessionId);
        else
            return sharedScriptEngines;
    }

    /**
     * Gets bindings from the session if this is an in-session requests.
     */
    public Optional<Map<String,Object>> getBindingsAsMap(final RequestMessage message) {
        if (message.optionalSessionId().isPresent())
            return Optional.<Map<String,Object>>of(getGremlinSession(message.sessionId).getBindings());
        else
            return Optional.empty();
    }

    private static Map<String,Object> extractBindingsFromMessage(final RequestMessage msg) {
        final Map<String, Object> m = new HashMap<>();
        final Optional<Map<String,Object>> bindingsInMessage = msg.optionalArgs(Tokens.ARGS_BINDINGS);
        return bindingsInMessage.orElse(m);
    }

    /**
     * Finds a session or constructs a new one if it does not exist.
     */
    private GremlinSession getGremlinSession(final UUID sessionId) {
        final GremlinSession session;
        if (sessionedScriptEngines.containsKey(sessionId))
            session = sessionedScriptEngines.get(sessionId);
        else {
            session = new GremlinSession(sessionId, settings);
            synchronized (this) { sessionedScriptEngines.put(sessionId, session); }
        }
        return session;
    }

    /**
     * An {@link FunctionalInterface} that throws {@code ScriptEngine} oriented exceptions.
     * @param <T> value passed to the function
     * @param <R> value returned from the function
     */
    @FunctionalInterface
    public interface EvalFunctionThatThrows<T, R> {
        R apply(T t) throws ScriptException, InterruptedException, ExecutionException, TimeoutException;
    }

    /**
     * An in-session implementation of {@link ScriptEngineOps}.
     */
    public class GremlinSession implements ScriptEngineOps {
        private final Bindings bindings;

        /**
         * Each session gets its own ScriptEngine so as to isolate its configuration and the classes loaded to it.
         * This is important as it enables user interfaces built on Gremlin Server to have isolation in what
         * libraries they use and what classes exist.
         */
        private final ScriptEngines scriptEngines;

        /**
         * By binding the session to run ScriptEngine evaluations in a specific thread, each request will respect
         * the ThreadLocal nature of Graph implementations.
         */
        private final ExecutorService executor = Executors.newSingleThreadExecutor();

        /**
         * Time to wait for a script to evaluate.
         */
        private final long scriptEvaluationTimeout;

        public GremlinSession(final UUID session, final Settings settings) {
            logger.info("New session established for {}", session);
            this.bindings = new SimpleBindings();
            this.scriptEngines = createScriptEngine(settings);
            sessionedScriptEngines.put(session, this);
            this.scriptEvaluationTimeout = settings.scriptEvaluationTimeout;
        }

        @Override
        public Object eval(final String script, final Bindings bindings, final String language)
                throws ScriptException, InterruptedException, ExecutionException, TimeoutException {
            // apply the submitted bindings to the server side ones.
            this.bindings.putAll(bindings);
            final Future<Object> future = executor.submit(
                    (Callable<Object>) () -> scriptEngines.eval(script, this.bindings, language));

            return future.get(this.scriptEvaluationTimeout, TimeUnit.MILLISECONDS);
        }

        @Override
        public Map<String, List<Map>> dependencies() {
            return scriptEngines.dependencies();
        }

        @Override
        public void addImports(final Set<String> imports) {
            scriptEngines.addImports(imports);
        }

        @Override
        public void use(final String group, final String artifact, final String version) {
            scriptEngines.use(group, artifact, version);
        }

        @Override
        public Map<String, List<Map>> imports() {
            return scriptEngines.imports();
        }

        @Override
        public void reset() {
            scriptEngines.reset();
        }

        public Bindings getBindings() {
            return bindings;
        }
    }
}