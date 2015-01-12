package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.util.function.TriConsumer;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptEngineLambda implements Function, Supplier, Consumer, Predicate, BiConsumer, TriConsumer {

    private static final String A = "a";
    private static final String B = "b";
    private static final String C = "c";

    protected final ScriptEngine engine;
    protected final String script;

    public ScriptEngineLambda(final String engineName, final String script) {
        this.engine = ScriptEngineCache.get(engineName);
        this.script = script;
    }

    public Object apply(final Object a) {
        try {
            final Bindings bindings = new SimpleBindings();
            bindings.put(A, a);
            return this.engine.eval(this.script, bindings);
        } catch (final ScriptException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public Object get() {
        try {
            return this.engine.eval(this.script);
        } catch (final ScriptException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public void accept(final Object a) {
        try {
            final Bindings bindings = new SimpleBindings();
            bindings.put(A, a);
            this.engine.eval(this.script, bindings);
        } catch (final ScriptException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public void accept(final Object a, final Object b) {
        try {
            final Bindings bindings = new SimpleBindings();
            bindings.put(A, a);
            bindings.put(B, b);
            this.engine.eval(this.script, bindings);
        } catch (final ScriptException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public void accept(final Object a, final Object b, final Object c) {
        try {
            final Bindings bindings = new SimpleBindings();
            bindings.put(A, a);
            bindings.put(B, b);
            bindings.put(C, c);
            this.engine.eval(this.script, bindings);
        } catch (final ScriptException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public boolean test(final Object a) {
        try {
            final Bindings bindings = new SimpleBindings();
            bindings.put(A, a);
            return (boolean) this.engine.eval(this.script, bindings);
        } catch (final ScriptException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

}
