package com.tinkerpop.gremlin.process.computer.util;


import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum LambdaType {

    CLASS {
        @Override
        public <S> Pair<Class<? extends S>, S> get(final Configuration configuration, final String key) {
            if (!configuration.containsKey(key))
                throw new IllegalArgumentException("The provided configuration does not have the supplier key: " + key);
            try {
                final Class<? extends S> klass = (Class<? extends S>) Class.forName(configuration.getString(key));
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
        public <S> Pair<S, S> get(final Configuration configuration, final String key) {
            if (!configuration.containsKey(key))
                throw new IllegalArgumentException("The provided configuration does not have the supplier key: " + key);
            return Pair.with((S) configuration.getProperty(key), (S) configuration.getProperty(key));
        }

        @Override
        public void set(final Configuration configuration, final String supplierTypeKey, final String key, final Object supplierObject) {
            configuration.setProperty(supplierTypeKey, this.name());
            configuration.setProperty(key, supplierObject);
        }
    }, SCRIPT {
        @Override
        public <S> Pair<String[], S> get(final Configuration configuration, final String key) {
            if (!configuration.containsKey(key))
                throw new IllegalArgumentException("The provided configuration does not have the supplier key: " + key);
            final String[] script = configuration.getString(key).split(SPLIT_TOKEN);
            return Pair.with(script, (S) new ScriptEngineLambda(script[0], script[1]));
        }

        @Override
        public void set(final Configuration configuration, final String supplierTypeKey, final String key, final Object supplierScript) {
            configuration.setProperty(supplierTypeKey, this.name());
            configuration.setProperty(key, ((String[]) supplierScript)[0] + SPLIT_TOKEN + ((String[]) supplierScript)[1]);
        }
    };

    private static final String SPLIT_TOKEN = Graph.System.system("");

    public static LambdaType getType(final Configuration configuration, final String key) {
        return configuration.containsKey(key) ? LambdaType.valueOf(configuration.getString(key)) : null;
    }

    public abstract <S> Pair<?, S> get(final Configuration configuration, final String key);

    public abstract void set(final Configuration configuration, final String supplierTypeKey, final String key, final Object supplier);


}
