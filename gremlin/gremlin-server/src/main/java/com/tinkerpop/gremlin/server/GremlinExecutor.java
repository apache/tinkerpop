package com.tinkerpop.gremlin.server;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.blueprints.tinkergraph.TinkerGraph;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinExecutor {
    private static final Logger logger = LoggerFactory.getLogger(GremlinExecutor.class);

    /**
     * Used in sessionless mode and centrally configured for imports/scripts.
     */
    private static final GroovyScriptEngineImpl sharedScriptEngine = new GroovyScriptEngineImpl();

    /**
     * Script engines are evaluated in a per session context where imports/scripts are isolated per session.
     */
    private static final Map<UUID, GremlinSession> sessionedScriptEngines = new ConcurrentHashMap<>();

    private static Optional<GremlinExecutor> singleton  = Optional.empty();

    private GremlinExecutor(){}

    public static GremlinExecutor instance() {
        if (!singleton.isPresent())
            singleton = Optional.of(new GremlinExecutor());
        return singleton.get();
    }

    public Object eval(final RequestMessage message, final GremlinServer.Graphs graphs) {
        return select(message, graphs).apply(message);
    }

    /**
     * Determine whether to execute the script by way of a specific session or by the shared script engine in
     * a sessionless request.
     */
    private Function<RequestMessage, Object> select(final RequestMessage message, final GremlinServer.Graphs graphs) {
        final Bindings bindings = new SimpleBindings();
        bindings.putAll(extractBindingsFromMessage(message));

        if (message.optionalSessionId().isPresent()) {
            // an in session request...throw in a dummy graph instance for now..............................
            final Graph g = TinkerFactory.createClassic();
            bindings.put("g", g);

            final GremlinSession session = getGremlinSession(message.sessionId, bindings);
            if (logger.isDebugEnabled()) logger.debug("Using session {} ScriptEngine to process {}", message.sessionId, message);
            return s -> session.eval(message.<String>optionalArgs("gremlin").get(), bindings);
        } else {
            // a sessionless request
            if (logger.isDebugEnabled()) logger.debug("Using shared ScriptEngine to process {}", message);
            return s -> {
                // put all the preconfigured graphs on the bindings
                bindings.putAll(graphs.getGraphs());

                try {
                    // do a safety cleanup of previous transaction...if any
                    graphs.rollbackAll();
                    final Object o = sharedScriptEngine.eval(message.<String>optionalArgs("gremlin").get(), bindings);
                    graphs.commitAll();
                    return o;
                } catch (ScriptException ex) {
                    // todo: gotta work on error handling for failed scripts..........
                    graphs.rollbackAll();
                    return null;
                }
            };
        }
    }

    private static Map<String,Object> extractBindingsFromMessage(final RequestMessage msg) {
        final Map<String, Object> m = new HashMap<>();
        final Optional<Map<String,Object>> bindingsInMessage = msg.optionalArgs("bindings");
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

    public class GremlinSession {
        private final Bindings bindings;

        /**
         * Each session gets its own ScriptEngine so as to isolate its configuration and the classes loaded to it.
         * This is important as it enables user interfaces built on Gremlin Server to have isolation in what
         * libraries they use and what classes exist.
         */
        private final GroovyScriptEngineImpl scriptEngine = new GroovyScriptEngineImpl();

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

        public Object eval(final String script, final Bindings bindings)  {
            try {
                // apply the submitted bindings to the server side ones.
                this.bindings.putAll(bindings);
                final Future<Object> future = executor.submit(() -> {
                    try {
                        return scriptEngine.eval(script, this.bindings);
                    } catch (Exception ex) {
                        return null;
                    }
                });
                return future.get();
            } catch (Exception ex) {
                return null;
            }
        }
    }
}
