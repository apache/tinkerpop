package com.tinkerpop.gremlin.server;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.HashMap;
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
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinExecutor {
    private static final Logger logger = LoggerFactory.getLogger(GremlinExecutor.class);

    /**
     * Used in sessionless mode and centrally configured for imports/scripts.
     */
    private static final GremlinGroovyScriptEngine sharedScriptEngine = new GremlinGroovyScriptEngine();

    /**
     * Script engines are evaluated in a per session context where imports/scripts are isolated per session.
     */
    private static final Map<UUID, GremlinSession> sessionedScriptEngines = new ConcurrentHashMap<>();

    public Object eval(final RequestMessage message, final GremlinServer.Graphs graphs)
            throws ScriptException, InterruptedException, ExecutionException {
        return select(message, graphs).apply(message);
    }

    /**
     * Determine whether to execute the script by way of a specific session or by the shared script engine in
     * a sessionless request.
     */
    private FunctionThatThrows<RequestMessage, Object> select(final RequestMessage message, final GremlinServer.Graphs graphs) {
        final Bindings bindings = new SimpleBindings();
        bindings.putAll(extractBindingsFromMessage(message));

        if (message.optionalSessionId().isPresent()) {
            // an in session request...throw in a dummy graph instance for now..............................
            final Graph g = TinkerFactory.createClassic();
            bindings.put("g", g);

            final GremlinSession session = getGremlinSession(message.sessionId, bindings);
            if (logger.isDebugEnabled()) logger.debug("Using session {} ScriptEngine to process {}", message.sessionId, message);
            return s -> session.eval(message.<String>optionalArgs(RequestMessage.FIELD_GREMLIN).get(), bindings);
        } else {
            // a sessionless request
            if (logger.isDebugEnabled()) logger.debug("Using shared ScriptEngine to process {}", message);
            return s -> {
                // put all the preconfigured graphs on the bindings
                bindings.putAll(graphs.getGraphs());

                try {
                    // do a safety cleanup of previous transaction...if any
                    graphs.rollbackAll();
                    final Object o = sharedScriptEngine.eval(message.<String>optionalArgs(RequestMessage.FIELD_GREMLIN).get(), bindings);
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
    private GremlinSession getGremlinSession(final UUID sessionId, Bindings bindings) {
        final GremlinSession session;
        if (sessionedScriptEngines.containsKey(sessionId))
            session = sessionedScriptEngines.get(sessionId);
        else {
            session = new GremlinSession(sessionId, bindings);
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
        private final GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine();

        /**
         * By binding the session to run ScriptEngine evaluations in a specific thread, each request will respect
         * the ThreadLocal nature of Graph implementations.
         */
        private final ExecutorService executor = Executors.newSingleThreadExecutor();

        public GremlinSession(final UUID session, final Bindings initialBindings) {
            logger.info("New session established for {}", session);
            this.bindings = initialBindings;
            sessionedScriptEngines.put(session, this);
        }

        public Object eval(final String script, final Bindings bindings)
                throws ScriptException, InterruptedException, ExecutionException {
            // apply the submitted bindings to the server side ones.
            this.bindings.putAll(bindings);
            final Future<Object> future = executor.submit(
                    (Callable<Object>) () -> scriptEngine.eval(script, this.bindings));

            // todo: do a configurable timeout here???
            return future.get();
        }
    }
}
