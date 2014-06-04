package com.tinkerpop.gremlin.groovy.engine;

import com.tinkerpop.gremlin.groovy.DefaultImportCustomizerProvider;
import com.tinkerpop.gremlin.groovy.jsr223.DependencyManager;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Holds a batch of the configured {@code ScriptEngine} objects for the server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ScriptEngines {
	private static final Logger logger = LoggerFactory.getLogger(ScriptEngines.class);

	/**
	 * {@code ScriptEngine} objects configured for the server keyed on the language name.
	 */
	private Map<String, ScriptEngine> scriptEngines = new ConcurrentHashMap<>();

	private static final GremlinGroovyScriptEngineFactory gremlinGroovyScriptEngineFactory = new GremlinGroovyScriptEngineFactory();

	/**
	 * Evaluate a script with {@code Bindings} for a particular language.
	 */
	public Object eval(final String script, final Bindings bindings, final String language)
			throws ScriptException, TimeoutException {
		if (!scriptEngines.containsKey(language))
			throw new IllegalArgumentException("Language [%s] not configured on this server.");

		return scriptEngines.get(language).eval(script, bindings);
	}

	/**
	 * Evaluate a script with {@code Bindings} for a particular language.
	 */
	public Object eval(final Reader reader, final Bindings bindings, final String language)
			throws ScriptException, TimeoutException {
		if (!scriptEngines.containsKey(language))
			throw new IllegalArgumentException("Language [%s] not configured on this server.");

		return scriptEngines.get(language).eval(reader, bindings);
	}

	/**
	 * Reload a {@code ScriptEngine} with fresh imports.
	 */
	public void reload(final String language, final Set<String> imports, final Set<String> staticImports) {
		// TODO: request a block on eval when reloading a language script engine.

		if (scriptEngines.containsKey(language))
			scriptEngines.remove(language);

		final ScriptEngine scriptEngine = createScriptEngine(language, imports, staticImports).orElseThrow(() -> new IllegalArgumentException("Language [%s] not supported."));
		scriptEngines.put(language, scriptEngine);
	}

	public ScriptEngine get(final String language) {
		return scriptEngines.get(language);
	}

	/**
	 * Perform append to the existing import list for all {@code ScriptEngine} instances that implement the
	 * {@link DependencyManager} interface.
	 */
	public void addImports(final Set<String> imports) {
		getDependencyManagers().forEach(dm -> dm.addImports(imports));
	}

	/**
	 * Pull in dependencies given some Maven coordinates.  Cycle through each {@code ScriptEngine} and determine if it
	 * implements {@link DependencyManager}.  For those that do call the @{link DependencyManager#use} method to fire
	 * it up.
	 */
	public void use(final String group, final String artifact, final String version) {
		getDependencyManagers().forEach(dm -> dm.use(group, artifact, version));
	}

	// todo: does reset kill init script work...probably need to re-init
	public void reset() {
		getDependencyManagers().forEach(dm -> dm.reset());
	}

	/**
	 * List dependencies for those {@code ScriptEngine} objects that implement the {@link DependencyManager} interface.
	 */
	public Map<String, List<Map>> dependencies() {
		final Map m = new HashMap();
		scriptEngines.entrySet().stream()
				.filter(kv -> kv.getValue() instanceof DependencyManager)
				.forEach(kv -> m.put(kv.getKey(), Arrays.asList(((DependencyManager) kv.getValue()).dependencies())));
		return m;
	}

	public Map<String, List<Map>> imports() {
		final Map m = new HashMap();
		scriptEngines.entrySet().stream()
				.filter(kv -> kv.getValue() instanceof DependencyManager)
				.forEach(kv -> m.put(kv.getKey(), Arrays.asList(((DependencyManager) kv.getValue()).imports())));
		return m;
	}

	/**
	 * Get the set of {@code ScriptEngine} that implement {@link DependencyManager} interface.
	 */
	private Set<DependencyManager> getDependencyManagers() {
		return scriptEngines.entrySet().stream()
				.map(kv -> kv.getValue())
				.filter(se -> se instanceof DependencyManager)
				.map(se -> (DependencyManager) se)
				.collect(Collectors.<DependencyManager>toSet());
	}

	private static Optional<ScriptEngine> createScriptEngine(final String language, final Set<String> imports,
															 final Set<String> staticImports) {
		if (language.equals(gremlinGroovyScriptEngineFactory.getLanguageName())) {
			// gremlin-groovy gets special initialization for custom imports and such.  could implement this more
			// generically with the DependencyManager interface, but going to wait to see how other ScriptEngines
			// develop for TinkerPop3 before committing too deeply here to any specific way of doing this.
			return Optional.of((ScriptEngine) new GremlinGroovyScriptEngine(1500,
					new DefaultImportCustomizerProvider(imports, staticImports)));
		} else {
			final ScriptEngineManager manager = new ScriptEngineManager();
			return Optional.ofNullable(manager.getEngineByName(language));
		}
	}

}
