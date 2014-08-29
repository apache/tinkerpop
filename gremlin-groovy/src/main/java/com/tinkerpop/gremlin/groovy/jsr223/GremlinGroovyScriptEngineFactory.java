package com.tinkerpop.gremlin.groovy.jsr223;

import com.tinkerpop.gremlin.util.Gremlin;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinGroovyScriptEngineFactory implements ScriptEngineFactory {

    private static final String ENGINE_NAME = "gremlin-groovy";
    private static final String LANGUAGE_NAME = "gremlin-groovy";
    private static final String PLAIN = "plain";
    private static final List<String> EXTENSIONS = Arrays.asList("groovy");

    @Override
    public String getEngineName() {
        return ENGINE_NAME;
    }

    @Override
    public String getEngineVersion() {
        return Gremlin.version();
    }

    @Override
    public List<String> getExtensions() {
        return EXTENSIONS;
    }

    @Override
    public String getLanguageName() {
        return LANGUAGE_NAME;
    }

    @Override
    public String getLanguageVersion() {
        return Gremlin.version();
    }

    @Override
    public String getMethodCallSyntax(final String obj, final String m, final String... args) {
        return null;
    }

    @Override
    public List<String> getMimeTypes() {
        return Arrays.asList(PLAIN);
    }

    @Override
    public List<String> getNames() {
        return Arrays.asList(LANGUAGE_NAME);
    }

    @Override
    public String getOutputStatement(final String toDisplay) {
        return "println " + toDisplay;
    }

    @Override
    public Object getParameter(final String key) {
        if (key.equals(ScriptEngine.ENGINE)) {
            return this.getEngineName();
        } else if (key.equals(ScriptEngine.ENGINE_VERSION)) {
            return this.getEngineVersion();
        } else if (key.equals(ScriptEngine.NAME)) {
            return ENGINE_NAME;
        } else if (key.equals(ScriptEngine.LANGUAGE)) {
            return this.getLanguageName();
        } else if (key.equals(ScriptEngine.LANGUAGE_VERSION)) {
            return this.getLanguageVersion();
        } else
            return null;
    }

    @Override
    public String getProgram(final String... statements) {
        String program = "";

        for (String statement : statements) {
            program = program + statement + "\n";
        }

        return program;
    }

    @Override
    public ScriptEngine getScriptEngine() {
        return new GremlinGroovyScriptEngine();
    }
}