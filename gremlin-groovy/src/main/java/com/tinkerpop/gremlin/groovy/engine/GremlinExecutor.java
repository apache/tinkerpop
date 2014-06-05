package com.tinkerpop.gremlin.groovy.engine;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Execute Gremlin scripts against a {@code ScriptEngine} instance.  It is designed to host any JSR-223 enabled
 * {@code ScriptEngine} and assumes such engines are designed to be thread-safe in the evaluation.  Script evaluation
 * functions return a {@link CompletableFuture} where scripts may timeout if their evaluation
 * takes too long.  The default timeout is 8000ms.
 * <br/>
 * By default, the {@code GremlinExecutor} initializes itself to use a shared thread pool initialized with four
 * threads. This default thread pool is shared for both the task of executing script evaluations and for scheduling
 * timeouts. It is worth noting that a timeout simply triggers the returned {@link CompletableFuture} to abort, but
 * the thread processing the script will continue to evaluate until completion.  This offers only marginal protection
 * against run-away scripts.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinExecutor {
	private static final Logger logger = LoggerFactory.getLogger(GremlinExecutor.class);

	/**
	 * {@link ScriptEngines} instance to evaluate Gremlin script requests.
	 */
	private ScriptEngines scriptEngines;

	private final Map<String, EngineSettings> settings;
	private final long scriptEvaluationTimeout;
	private final Bindings globalBindings;
	private final List<List<String>> use;
	private final ExecutorService executorService;
	private final ScheduledExecutorService scheduledExecutorService;
	private final Consumer<Bindings> beforeEval;
	private final Consumer<Bindings> afterSuccess;
	private final Consumer<Bindings> afterTimeout;
	private final BiConsumer<Bindings,Exception> afterFailure;

	private GremlinExecutor(final Map<String, EngineSettings> settings, final List<List<String>> use,
							final long scriptEvaluationTimeout, final Bindings globalBindings,
							final ExecutorService executorService, final ScheduledExecutorService scheduledExecutorService,
							final Consumer<Bindings> beforeEval, final Consumer<Bindings> afterSuccess,
							final Consumer<Bindings> afterTimeout, final BiConsumer<Bindings,Exception> afterFailure) {
		if (logger.isDebugEnabled())
			logger.debug("Initializing GremlinExecutor.  This should not happen more than once.");

		this.executorService = executorService;
		this.scheduledExecutorService = scheduledExecutorService;
		this.beforeEval = beforeEval;
		this.afterSuccess = afterSuccess;
		this.afterTimeout = afterTimeout;
		this.afterFailure = afterFailure;
		this.use = use;
		this.settings = settings;
		this.scriptEvaluationTimeout = scriptEvaluationTimeout;
		this.globalBindings = globalBindings;
		this.scriptEngines = createScriptEngines();
	}

	public CompletableFuture<Object> eval(final String script) {
		return eval(script, Optional.empty(), new SimpleBindings());
	}

	public CompletableFuture<Object> eval(final String script, final Bindings boundVars) {
		return eval(script, Optional.empty(), boundVars);
	}

	public CompletableFuture<Object> eval(final String script, final Map<String,Object> boundVars) {
		return eval(script, Optional.empty(), new SimpleBindings(boundVars));
	}

	public CompletableFuture<Object> eval(final String script, final Optional<String> language, final Bindings boundVars) {
		final Bindings bindings = new SimpleBindings();
		bindings.putAll(this.globalBindings);
		bindings.putAll(boundVars);

		final String lang = language.orElse("gremlin-groovy");

		if (logger.isDebugEnabled()) logger.debug("Preparing to evaluate script - {} - in thread [{}]", script, Thread.currentThread().getName());

		// select the gremlin threadpool to execute the script evaluation in
		final AtomicBoolean abort = new AtomicBoolean(false);
		final CompletableFuture<Object> future = CompletableFuture.supplyAsync(() -> {
			try {
				if (logger.isDebugEnabled()) logger.debug("Evaluating script - {} - in thread [{}]", script, Thread.currentThread().getName());

				beforeEval.accept(bindings);
				final Object o = scriptEngines.eval(script, bindings, lang);

				if (abort.get())
					afterTimeout.accept(bindings);
				else
					afterSuccess.accept(bindings);

				return o;
			} catch (Exception ex) {
				afterFailure.accept(bindings, ex);
				throw new RuntimeException(ex);
			}
		}, executorService);

		scheduleTimeout(future, script, abort);

		return future;
	}

	public ScriptEngines getScriptEngines() {
		return this.scriptEngines;
	}

	private void scheduleTimeout(final CompletableFuture<Object> evaluationFuture, final String script, final AtomicBoolean abort) {
		if (scriptEvaluationTimeout > 0) {
			// Schedule a timeout in the io threadpool for future execution - killing an eval is cheap
			final ScheduledFuture<?> sf = scheduledExecutorService.schedule(() -> {
				if (logger.isDebugEnabled())
					logger.info("Timing out script - {} - in thread [{}]", script, Thread.currentThread().getName());

				if (!evaluationFuture.isDone()) {
					abort.set(true);
					evaluationFuture.completeExceptionally(new TimeoutException(
							String.format("Script evaluation exceeded the configured threshold of %s ms for request [%s]", scriptEvaluationTimeout, script)));
				}
			}, scriptEvaluationTimeout, TimeUnit.MILLISECONDS);

			// Cancel the scheduled timeout if the eval future is complete.
			evaluationFuture.thenRun(() -> sf.cancel(false));
		}
	}

	private ScriptEngines createScriptEngines() {
		final ScriptEngines scriptEngines = new ScriptEngines();
		for (Map.Entry<String, EngineSettings> config : settings.entrySet()) {
			final String language = config.getKey();
			scriptEngines.reload(language, new HashSet<>(config.getValue().getImports()),
					new HashSet<>(config.getValue().getStaticImports()));
		}

		use.forEach(u -> {
			if (u.size() != 3)
				logger.warn("Could not resolve dependencies for [{}].  Each entry for the 'use' configuration must include [groupId, artifactId, version]", u);
			else {
				logger.info("Getting dependencies for [{}]", u);
				scriptEngines.use(u.get(0), u.get(1), u.get(2));
			}
		});

		// initialization script eval must occur after dependencies are set with "use"
		for (Map.Entry<String, EngineSettings> config : settings.entrySet()) {
			final String language = config.getKey();

			// script engine initialization files that fail will only log warnings - not fail server initialization
			final AtomicBoolean hasErrors = new AtomicBoolean(false);
			config.getValue().getScripts().stream().map(File::new).filter(f -> {
				if (!f.exists()) {
					logger.warn("Could not initialize {} ScriptEngine with {} as file does not exist", language, f);
					hasErrors.set(true);
				}

				return f.exists();
			}).map(f -> {
				try {
					return Pair.with(f, Optional.of(new FileReader(f)));
				} catch (IOException ioe) {
					logger.warn("Could not initialize {} ScriptEngine with {} as file could not be read - {}", language, f, ioe.getMessage());
					hasErrors.set(true);
					return Pair.with(f, Optional.<FileReader>empty());
				}
			}).filter(p -> p.getValue1().isPresent()).map(p -> Pair.with(p.getValue0(), p.getValue1().get())).forEachOrdered(p -> {
				try {
					final Bindings bindings = new SimpleBindings();
					bindings.putAll(this.globalBindings);
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

	/**
	 * Create a {@code Builder} with the gremlin-groovy ScriptEngine configured.
	 */
	public static Builder create() {
		return new Builder().addEngineSettings("gremlin-groovy", new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
	}

	/**
	 * Create a {@code Builder} and specify the first ScriptEngine to be included.
	 */
	public static Builder create(final String engineName, final List<String> imports,
								 final List<String> staticImports, final List<String> scripts) {
		return new Builder().addEngineSettings(engineName, imports, staticImports, scripts);
	}

	public static class Builder {
		private long scriptEvaluationTimeout = 8000;
		private Map<String, EngineSettings> settings = new HashMap<>();
		private ExecutorService executorService;
		private ScheduledExecutorService scheduledExecutorService;
		private Consumer<Bindings> beforeEval = (b) -> {};
		private Consumer<Bindings> afterSuccess = (b) -> {};
		private Consumer<Bindings> afterTimeout = (b) -> {};
		private BiConsumer<Bindings,Exception> afterFailure = (b,e) -> {};
		private List<List<String>> use = new ArrayList<>();
		private Bindings globalBindings = new SimpleBindings();

		private Builder() {
			final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("gremlin-executor-%d").build();
			this.scheduledExecutorService = Executors.newScheduledThreadPool(4, threadFactory);
			this.executorService = scheduledExecutorService;
		}

		/**
		 * Add a particular script engine for the executor to instantiate.
		 *
		 * @param engineName The name of the engine as defined by the engine itself.
		 * @param imports A list of imports for the engine.
		 * @param staticImports A list of static imports for the engine.
		 * @param scripts A list of scripts to execute in the engine to initialize it.
		 */
		public Builder addEngineSettings(final String engineName, final List<String> imports,
										 final List<String> staticImports, final List<String> scripts) {
			if (null == imports) throw new IllegalArgumentException("imports cannot be null");
			if (null == staticImports) throw new IllegalArgumentException("staticImports cannot be null");
			if (null == scripts) throw new IllegalArgumentException("scripts cannot be null");

			settings.put(engineName, new EngineSettings(imports, staticImports, scripts));
			return this;
		}

		/**
		 * Bindings to apply to every script evaluated.
		 */
		public Builder globalBindings(final Bindings bindings) {
			this.globalBindings = bindings;
			return this;
		}

		/**
		 * Amount of time a script has before it times out.
		 *
		 * @param scriptEvaluationTimeout Time in milliseconds that a script is allowed to run.
		 */
		public Builder scriptEvaluationTimeout(final long scriptEvaluationTimeout) {
			this.scriptEvaluationTimeout = scriptEvaluationTimeout;
			return this;
		}

		/**
		 * Replaces any settings provided by {@link #engineSettings(java.util.Map)}.
		 */
		public Builder engineSettings(final Map<String, EngineSettings> settings) {
			this.settings = settings;
			return this;
		}

		/**
		 * The thread pool used to evaluate scripts.
		 */
		public Builder executorService(final ExecutorService executorService) {
			this.executorService = executorService;
			return this;
		}

		/**
		 * The thread pool used to schedule timeouts on scripts.
		 */
		public Builder scheduledExecutorService(final ScheduledExecutorService scheduledExecutorService) {
			this.scheduledExecutorService = scheduledExecutorService;
			return this;
		}

		/**
		 * A {@link Consumer} to execute just before the script evaluation.
		 */
		public Builder beforeEval(final Consumer<Bindings> beforeEval) {
			this.beforeEval = beforeEval;
			return this;
		}

		/**
		 * A {@link Consumer} to execute just after successful script evaluation.
		 */
		public Builder afterSuccess(final Consumer<Bindings> afterSuccess) {
			this.afterSuccess = afterSuccess;
			return this;
		}

		/**
		 * A {@link Consumer} to execute if the script timesout
		 */
		public Builder afterTimeout(final Consumer<Bindings> afterTimeout) {
			this.afterTimeout = afterTimeout;
			return this;
		}

		/**
		 * A {@link Consumer} to execute in the event of failure.
		 */
		public Builder afterFailure(final BiConsumer<Bindings, Exception> afterFailure) {
			this.afterFailure = afterFailure;
			return this;
		}

		/**
		 * A set of maven coordinates for dependencies to be applied for the script engine instances.
		 */
		public Builder setUse(final List<List<String>> use) {
			this.use = use;
			return this;
		}

		public GremlinExecutor build() {
			return new GremlinExecutor(settings, use, scriptEvaluationTimeout, globalBindings, executorService,
					scheduledExecutorService, beforeEval, afterSuccess, afterTimeout, afterFailure);
		}
	}

	private static class EngineSettings {
		private List<String> imports;
		private List<String> staticImports;
		private List<String> scripts;

		public EngineSettings(final List<String> imports, final List<String> staticImports, final List<String> scripts) {
			this.imports = imports;
			this.staticImports = staticImports;
			this.scripts = scripts;
		}

		private List<String> getImports() {
			return imports;
		}

		private List<String> getStaticImports() {
			return staticImports;
		}

		private List<String> getScripts() {
			return scripts;
		}
	}
}
