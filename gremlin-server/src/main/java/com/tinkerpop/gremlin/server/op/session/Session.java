package com.tinkerpop.gremlin.server.op.session;

import com.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.Graphs;
import com.tinkerpop.gremlin.server.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Session {
    private static final Logger logger = LoggerFactory.getLogger(Session.class);
    private final Bindings bindings;
    private final Settings settings;
    private final Graphs graphs;
    private final String session;
    private final ScheduledExecutorService scheduledExecutorService;
    private final long configuredSessionTimeout;

    private AtomicReference<ScheduledFuture> kill = new AtomicReference<>();

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

    private final ConcurrentHashMap<String,Session> sessions;

    public Session(final String session, final Context context, final ConcurrentHashMap<String,Session> sessions) {
        logger.info("New session established for {}", session);
        this.session = session;
        this.bindings = new SimpleBindings();
        this.settings = context.getSettings();
        this.graphs = context.getGraphs();
        this.scheduledExecutorService = context.getScheduledExecutorService();
        this.sessions = sessions;

        final Settings.ProcessorSettings processorSettings = this.settings.processors.stream()
                .filter(p -> p.className.equals(SessionOpProcessor.class.getCanonicalName()))
                .findFirst().orElse(SessionOpProcessor.DEFAULT_SETTINGS);
        this.configuredSessionTimeout = Long.parseLong(processorSettings.config.get(SessionOpProcessor.CONFIG_SESSION_TIMEOUT).toString());

        this.gremlinExecutor =  initializeGremlinExecutor().build();
    }

    public GremlinExecutor getGremlinExecutor() {
        return gremlinExecutor;
    }

    public Bindings getBindings() {
        return bindings;
    }

    public void touch() {
        // if the task of killing is cancelled successfully then reset the session monitor. otherwise this session
        // has already been killed and there's nothing left to do with tis session.
        final ScheduledFuture killFuture = kill.get();
        if (null == killFuture || !killFuture.isDone()) {
            if (killFuture != null) killFuture.cancel(false);
            kill.set(this.scheduledExecutorService.schedule(() -> {
                sessions.remove(this.session);
                logger.info("Kill idle session named {} after {} milliseconds", this.session, this.configuredSessionTimeout);
            }, this.configuredSessionTimeout, TimeUnit.MILLISECONDS));
        }
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
