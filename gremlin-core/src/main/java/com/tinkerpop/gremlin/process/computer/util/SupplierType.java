package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum SupplierType {

    CLASS {
        @Override
        public <S> Pair<Class<? extends Supplier<S>>, Supplier<S>> get(final Configuration configuration, final String key) {
            if (!configuration.containsKey(key))
                throw new IllegalArgumentException("The provided configuration does not have the supplier key: " + key);
            try {
                final Class<? extends Supplier<S>> klass = (Class<? extends Supplier<S>>) Class.forName(configuration.getString(key));
                return Pair.with(klass, klass.newInstance());
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        @Override
        public void set(final Configuration configuration, final String supplierTypeKey, final String key, final Object supplierClass) {
            configuration.setProperty(supplierTypeKey, this.name());
            configuration.setProperty(key, ((Class) supplierClass).getCanonicalName());
        }
    }, OBJECT {
        @Override
        public <S> Pair<Supplier<S>, Supplier<S>> get(final Configuration configuration, final String key) {
            if (!configuration.containsKey(key))
                throw new IllegalArgumentException("The provided configuration does not have the supplier key: " + key);
            return Pair.with((Supplier<S>) configuration.getProperty(key), (Supplier<S>) configuration.getProperty(key));
        }

        @Override
        public void set(final Configuration configuration, final String supplierTypeKey, final String key, final Object supplierObject) {
            configuration.setProperty(supplierTypeKey, this.name());
            configuration.setProperty(key, supplierObject);
        }
    }, SCRIPT {
        @Override
        public <S> Pair<String[], Supplier<S>> get(final Configuration configuration, final String key) {
            if (!configuration.containsKey(key))
                throw new IllegalArgumentException("The provided configuration does not have the supplier key: " + key);
            final String[] script = configuration.getString(key).split(SPLIT_TOKEN);
            final ScriptEngine engine = new ScriptEngineManager().getEngineByName(script[0]);
            if (null == engine)
                throw new IllegalArgumentException("The provided script engine does not exist: " + script[0]);
            return Pair.with(script, () -> {
                try {
                    return (S) engine.eval(script[1]);
                } catch (Exception e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            });
        }

        @Override
        public void set(final Configuration configuration, final String supplierTypeKey, final String key, final Object supplierScript) {
            configuration.setProperty(supplierTypeKey, this.name());
            configuration.setProperty(key, ((String[]) supplierScript)[0] + SPLIT_TOKEN + ((String[]) supplierScript)[1]);
        }
    };

    private static final String SPLIT_TOKEN = Graph.System.system("");

    public static SupplierType getType(final Configuration configuration, final String key) {
        return SupplierType.valueOf(configuration.getString(key));
    }

    public abstract <S> Pair<?, Supplier<S>> get(final Configuration configuration, final String key);

    public abstract void set(final Configuration configuration, final String supplierTypeKey, final String key, final Object supplier);

}
