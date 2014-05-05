package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.driver.Tokens;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.ScheduledFuture;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Execute Gremlin scripts against a {@code ScriptEngine} instance.  The shared {@code ScriptEngine} can not be
 * dynamically re-initialized as doing so essentially equates to restarting the server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinExecutor {
    private static final Logger logger = LoggerFactory.getLogger(GremlinExecutor.class);

    /**
     * {@link ScriptEngines} instance to evaluate Gremlin script requests.
     */
    private ScriptEngines sharedScriptEngines;

    private Settings settings = null;

    /**
     * Construct the {@link GremlinExecutor} given the provided {@link Settings}.
     *
     * @param settings the Gremlin Server configuration
     */
    public GremlinExecutor(final Settings settings, final Graphs graphs) {
        if (logger.isDebugEnabled())
            logger.debug("Initializing GremlinExecutor.  This should not happen more than once.");
        sharedScriptEngines = createScriptEngines(settings, graphs);

        this.settings = settings;
    }

    /**
     * Evaluate the {@link com.tinkerpop.gremlin.driver.message.RequestMessage} within a {@code ScriptEngine} instance.
     *
     * @param message the current message
     * @param ctx the server context
     * @return the result from the evaluation
     */
    public CompletableFuture<Object> eval(final RequestMessage message, final Context ctx)
            throws ScriptException, InterruptedException, ExecutionException, TimeoutException {
        final Graphs graphs = ctx.getGraphs();
        final Bindings bindings = new SimpleBindings();
        bindings.putAll(extractBindingsFromMessage(message));

        final String language = message.<String>optionalArgs(Tokens.ARGS_LANGUAGE).orElse("gremlin-groovy");
        bindings.putAll(graphs.getGraphs());

        if (logger.isDebugEnabled()) logger.debug("Preparing to evaluate script - {} - in thread [{}]", message.<String>optionalArgs(Tokens.ARGS_GREMLIN).get(), Thread.currentThread().getName());

        // select the gremlin threadpool to execute the script evaluation in
        final EventExecutorGroup executorService = ctx.getChannelHandlerContext().executor().next();
        final AtomicBoolean abort = new AtomicBoolean(false);
        final CompletableFuture<Object> future = CompletableFuture.supplyAsync(() -> {
            try {
                if (logger.isDebugEnabled()) logger.debug("Evaluating script - {} - in thread [{}]", message.<String>optionalArgs(Tokens.ARGS_GREMLIN).get(), Thread.currentThread().getName());

                graphs.rollbackAll();
                final Object o = sharedScriptEngines.eval(message.<String>optionalArgs(Tokens.ARGS_GREMLIN).get(), bindings, language);

                // if the eval timed-out then it will have been "aborted", thus any action performed on the graph
                // must be rolledback
                if (abort.get())
                    graphs.rollbackAll();
                else
                    graphs.commitAll();

                return o;
            } catch (Exception ex) {
                graphs.rollbackAll();
                throw new RuntimeException(ex);
            }
        }, executorService);

        scheduleTimeout(ctx.getChannelHandlerContext(), future, message, abort);

        return future;
    }

    private void scheduleTimeout(final ChannelHandlerContext ctx, final CompletableFuture<Object> evaluationFuture,
                                 final RequestMessage requestMessage, final AtomicBoolean abort) {
        if (settings.scriptEvaluationTimeout > 0) {
            // Schedule a timeout in the io threadpool for future execution - killing an eval is cheap
            final ScheduledFuture<?> sf = ctx.channel().eventLoop().next().schedule(() -> {
                if (logger.isDebugEnabled()) logger.info("Timing out script - {} - in thread [{}]", requestMessage.<String>optionalArgs(Tokens.ARGS_GREMLIN).get(), Thread.currentThread().getName());

                if (!evaluationFuture.isDone()) {
                    abort.set(true);
                    evaluationFuture.completeExceptionally(new TimeoutException(String.format("Script evaluation exceeded the configured threshold of %s ms for request [%s]", settings.scriptEvaluationTimeout, requestMessage)));
                }
            }, settings.scriptEvaluationTimeout, TimeUnit.MILLISECONDS);

            // Cancel the scheduled timeout if the eval future is complete.
            evaluationFuture.thenRun(() -> sf.cancel(false));
        }
    }

    private synchronized static ScriptEngines createScriptEngines(final Settings settings, final Graphs graphs) {
        final ScriptEngines scriptEngines = new ScriptEngines();
        for (Map.Entry<String, Settings.ScriptEngineSettings> config : settings.scriptEngines.entrySet()) {
            final String language = config.getKey();
            scriptEngines.reload(language, new HashSet<>(config.getValue().imports),
                    new HashSet<>(config.getValue().staticImports));
        }

        settings.use.forEach(u -> {
            if (u.size() != 3)
                logger.warn("Could not resolve dependencies for [{}].  Each entry for the 'use' configuration must include [groupId, artifactId, version]", u);
            else {
                logger.info("Getting dependencies for [{}]", u);
                scriptEngines.use(u.get(0), u.get(1), u.get(2));
            }
        });

        // initialization script eval must occur after dependencies are set with "use"
        for (Map.Entry<String, Settings.ScriptEngineSettings> config : settings.scriptEngines.entrySet()) {
            final String language = config.getKey();

            // script engine initialization files that fail will only log warnings - not fail server initialization
            final AtomicBoolean hasErrors = new AtomicBoolean(false);
            config.getValue().scripts.stream().map(File::new).filter(f -> {
                if (!f.exists()) {
                    logger.warn("Could not initialize {} ScriptEngine with {} as file does not exist", language, f);
                    hasErrors.set(true);
                }

                return f.exists();
            }).map(f -> {
                try {
                    return Pair.with(f, Optional.<FileReader>of(new FileReader(f)));
                } catch (IOException ioe) {
                    logger.warn("Could not initialize {} ScriptEngine with {} as file could not be read - {}", language, f, ioe.getMessage());
                    hasErrors.set(true);
                    return Pair.with(f, Optional.<FileReader>empty());
                }
            }).filter(p -> p.getValue1().isPresent()).map(p -> Pair.with(p.getValue0(), p.getValue1().get())).forEachOrdered(p -> {
                try {
                    final Bindings bindings = new SimpleBindings();
                    bindings.putAll(graphs.getGraphs());
                    scriptEngines.get(language).eval(p.getValue1(), bindings);
                    logger.info("Initialized {} ScriptEngine with {}", language, p.getValue0());
                } catch (ScriptException sx) {
                    hasErrors.set(true);
                    logger.warn("Could not initialize {} ScriptEngine with {} as script could not be evaluated - {}", language, p.getValue0(), sx.getMessage());
                }
            });
        }

        return scriptEngines;
    }

    public ScriptEngines getSharedScriptEngines() {
        return this.sharedScriptEngines;
    }

    private static Map<String, Object> extractBindingsFromMessage(final RequestMessage msg) {
        final Map<String, Object> m = new HashMap<>();
        final Optional<Map<String, Object>> bindingsInMessage = msg.optionalArgs(Tokens.ARGS_BINDINGS);
        return bindingsInMessage.orElse(m);
    }
}