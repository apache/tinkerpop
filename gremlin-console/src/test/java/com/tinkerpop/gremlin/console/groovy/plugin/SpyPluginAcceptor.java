package com.tinkerpop.gremlin.console.groovy.plugin;

import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;

import javax.script.ScriptException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SpyPluginAcceptor implements PluginAcceptor {

    private final Set<String> imports = new HashSet<>();
    private final Map<String, Object> bindings = new HashMap<>();

    private final Function<String, Object> evalSpy;
    private final Supplier<Map<String, Object>> environment;

    public SpyPluginAcceptor() {
        this(script -> script);
    }

    public SpyPluginAcceptor(final Function<String, Object> evalSpy) {
        this(evalSpy, HashMap::new);
    }

    public SpyPluginAcceptor(final Supplier<Map<String, Object>> environmentMaker) {
        this(script -> script, environmentMaker);
    }

    public SpyPluginAcceptor(final Function<String, Object> evalSpy, final Supplier<Map<String, Object>> environmentMaker) {
        this.evalSpy = evalSpy;
        this.environment = environmentMaker;
    }

    @Override
    public void addImports(final Set<String> importStatements) {
        imports.addAll(importStatements);
    }

    @Override
    public void addBinding(final String key, final Object val) {
        bindings.put(key, val);
    }

    @Override
    public Map<String, Object> getBindings() {
        return bindings;
    }

    @Override
    public Object eval(final String script) throws ScriptException {
        return evalSpy.apply(script);
    }

    @Override
    public Map<String, Object> environment() {
        return environment.get();
    }

    public Set<String> getImports() {
        return imports;
    }
}
