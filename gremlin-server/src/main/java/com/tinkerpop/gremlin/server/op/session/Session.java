package com.tinkerpop.gremlin.server.op.session;

import com.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.Graphs;
import com.tinkerpop.gremlin.server.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Session {
    private static final Logger logger = LoggerFactory.getLogger(Session.class);
    private final Bindings bindings;
    private final Settings settings;
    private final Graphs graphs;
    private final ScheduledExecutorService scheduledExecutorService;

    /**
     * Each session gets its own ScriptEngine so as to isolate its configuration and the classes loaded to it.
     * This is important as it enables user interfaces built on Gremlin Server to have isolation in what
     * libraries they use and what classes exist.
     */
    private final GremlinExecutor gremlinExecutor;

    /**
     * By binding the session to run ScriptEngine evaluations in a specific thread, each request will respect
     * the ThreadLocal nature of Graph implementations.
     */
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public Session(final String session, final Context context) {
        logger.info("New session established for {}", session);
        this.bindings = new SimpleBindings();
        this.settings = context.getSettings();
        this.graphs = context.getGraphs();
        this.scheduledExecutorService = context.getScheduledExecutorService();

        this.gremlinExecutor =  initializeGremlinExecutor().build();
    }

    public GremlinExecutor getGremlinExecutor() {
        return gremlinExecutor;
    }

    public Bindings getBindings() {
        return bindings;
    }

    private GremlinExecutor.Builder initializeGremlinExecutor() {
        final GremlinExecutor.Builder gremlinExecutorBuilder = GremlinExecutor.create()
                .scriptEvaluationTimeout(settings.scriptEvaluationTimeout)
                .afterTimeout(b -> {
                    graphs.rollbackAll();
                    this.bindings.clear();
                    this.bindings.putAll(b);
                })
                .afterSuccess(b -> {
                    this.bindings.clear();
                    this.bindings.putAll(b);
                })
                .use(settings.use)
                .globalBindings(graphs.getGraphsAsBindings())
                .executorService(executor)
                .scheduledExecutorService(scheduledExecutorService);

        settings.scriptEngines.forEach((k, v) -> gremlinExecutorBuilder.addEngineSettings(k, v.imports, v.staticImports, v.scripts, v.config));

        return gremlinExecutorBuilder;
    }
}
