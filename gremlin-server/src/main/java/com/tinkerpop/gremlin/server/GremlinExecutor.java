package com.tinkerpop.gremlin.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    public GremlinExecutor(final Settings settings) {
        if (logger.isDebugEnabled())
            logger.debug("Initializing GremlinExecutor.  This should not happen more than once.");
        sharedScriptEngines = createScriptEngine(settings);

        this.settings = settings;
    }

    /**
     * Evaluate the {@link RequestMessage} within a {@code ScriptEngine} instance.
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

        final EventExecutorGroup executorService = ctx.getChannelHandlerContext().channel().eventLoop().next();
        try {

            final CompletableFuture<Object> future = CompletableFuture.supplyAsync(() -> {
                try {
                    graphs.rollbackAll();
                    final Object o = sharedScriptEngines.eval(message.<String>optionalArgs(Tokens.ARGS_GREMLIN).get(), bindings, language);
                    graphs.commitAll();

                    return o;
                } catch (Exception ex) {
                    graphs.rollbackAll();
                    return null;
                }

            }, executorService);

            scheduleTimeout(ctx.getChannelHandlerContext(), future, message);

            return future;
        } catch (Exception ex) {
            logger.warn("Script did not evaluate with success [{}]", message);

            // try to rollback the changes
            executorService.submit(graphs::rollbackAll).get();

            // throw the exception as it will be handled by the request processor and messaged back to the
            // calling client.
            throw ex;
        }
    }

    private void scheduleTimeout(final ChannelHandlerContext ctx, final CompletableFuture<Object> future,
                                 final RequestMessage requestMessage) {
        if (settings.scriptEvaluationTimeout > 0) {
            // Schedule a timeout.
            final ScheduledFuture<?> sf = ctx.executor().schedule(() -> {
                if (!future.isDone())
                    future.completeExceptionally(new TimeoutException(String.format("Script evaluation exceeded the configured threshold of %s ms for request [%s]", settings.scriptEvaluationTimeout, requestMessage)));
            }, settings.scriptEvaluationTimeout, TimeUnit.MILLISECONDS);

            // Cancel the scheduled timeout if the eval future is complete.
            future.thenRun(() -> sf.cancel(false));
        }
    }

    private synchronized static ScriptEngines createScriptEngine(final Settings settings) {
        final ScriptEngines scriptEngines = new ScriptEngines();
        for (Map.Entry<String, Settings.ScriptEngineSettings> config : settings.scriptEngines.entrySet()) {
            scriptEngines.reload(config.getKey(), new HashSet<>(config.getValue().imports),
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