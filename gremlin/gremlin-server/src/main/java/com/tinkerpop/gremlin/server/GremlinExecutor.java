package com.tinkerpop.gremlin.server;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Execute Gremlin scripts against a ScriptEngine instance.  The ScriptEngine maybe be a shared ScriptEngine in the
 * case of a sessionless request or a standalone ScriptEngine bound to a session that always executes within the same
 * thread for every request.
 * <p>
 * The shared ScriptEngine for sessionless requests can not be dynamically re-initialized as doing so essentially
 * equates to restarting the server.  It's easier to just do that.
 * <p>
 * The sessioned ScriptEngine is initialized with settings once per session and can be reset by the session itself.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinExecutor {
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

    public Object eval(final RequestMessage message, final GremlinServer.Graphs graphs)
            throws ScriptException, InterruptedException, ExecutionException {
        return select(message, graphs).apply(message);
    }

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
        return scriptEngines;
    }

    /**
     * Determines whether or not the GremlinExecutor was initialized with the appropriate settings or not.
     */
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Determine whether to execute the script by way of a specific session or by the shared script engine in
     * a sessionless request.
     */
    private FunctionThatThrows<RequestMessage, Object> select(final RequestMessage message, final GremlinServer.Graphs graphs) {
        final Bindings bindings = new SimpleBindings();
        bindings.putAll(extractBindingsFromMessage(message));

        final String language = message.<String>optionalArgs("language").orElse("gremlin-groovy");

        if (message.optionalSessionId().isPresent()) {
            // an in session request...throw in a dummy graph instance for now..............................
            final Graph g = TinkerFactory.createClassic();
            bindings.put("g", g);

            final GremlinSession session = getGremlinSession(message.sessionId, bindings);
            if (logger.isDebugEnabled()) logger.debug("Using session {} ScriptEngine to process {}", message.sessionId, message);
            return s -> session.eval(message.<String>optionalArgs(RequestMessage.FIELD_GREMLIN).get(), bindings, language);
        } else {
            // a sessionless request
            if (logger.isDebugEnabled()) logger.debug("Using shared ScriptEngine to process {}", message);
            return s -> {
                // put all the preconfigured graphs on the bindings
                bindings.putAll(graphs.getGraphs());

                try {
                    // do a safety cleanup of previous transaction...if any
                    graphs.rollbackAll();
                    final Object o = sharedScriptEngines.eval(message.<String>optionalArgs(RequestMessage.FIELD_GREMLIN).get(), bindings, language);
                    graphs.commitAll();
                    return o;
                } catch (ScriptException ex) {
                    // todo: gotta work on error handling for failed scripts..........
                    graphs.rollbackAll();
                    throw ex;
                }
            };
        }
    }

    private static Map<String,Object> extractBindingsFromMessage(final RequestMessage msg) {
        final Map<String, Object> m = new HashMap<>();
        final Optional<Map<String,Object>> bindingsInMessage = msg.optionalArgs(RequestMessage.FIELD_BINDINGS);
        return bindingsInMessage.orElse(m);
    }

    /**
     * Finds a session or constructs a new one if it does not exist.
     */
    private GremlinSession getGremlinSession(final UUID sessionId, final Bindings bindings) {
        final GremlinSession session;
        if (sessionedScriptEngines.containsKey(sessionId))
            session = sessionedScriptEngines.get(sessionId);
        else {
            session = new GremlinSession(sessionId, bindings, settings);
            synchronized (this) { sessionedScriptEngines.put(sessionId, session); }
        }
        return session;
    }

    @FunctionalInterface
    public interface FunctionThatThrows<T, R> {
        R apply(T t) throws ScriptException, InterruptedException, ExecutionException;
    }

    public class GremlinSession {
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

        public GremlinSession(final UUID session, final Bindings initialBindings, final Settings settings) {
            logger.info("New session established for {}", session);
            this.bindings = initialBindings;
            this.scriptEngines = createScriptEngine(settings);
            sessionedScriptEngines.put(session, this);
        }

        public Object eval(final String script, final Bindings bindings, final String language)
                throws ScriptException, InterruptedException, ExecutionException {
            // apply the submitted bindings to the server side ones.
            this.bindings.putAll(bindings);
            final Future<Object> future = executor.submit(
                    (Callable<Object>) () -> scriptEngines.eval(script, this.bindings, language));

            // todo: do a configurable timeout here???
            return future.get();
        }
    }
}
