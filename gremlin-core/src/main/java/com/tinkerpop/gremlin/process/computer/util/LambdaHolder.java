package com.tinkerpop.gremlin.process.computer.util;


import org.apache.commons.configuration.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LambdaHolder<S> {

    public enum Type {
        OBJECT,
        CLASS,
        SCRIPT
    }

    private Type type;
    private Object configObject;
    private Object realObject;
    private String configKeyPrefix;

    private static final String DOT_TYPE = ".type";
    private static final String DOT_OBJECT = ".object";


    public S get() {
        return (S) this.realObject;
    }

    private LambdaHolder() {

    }

    public static <T> LambdaHolder<T> storeState(final Configuration configuration, final Type type, final String configKeyPrefix, final Object configObject) {
        if (type.equals(Type.SCRIPT) && !(configObject instanceof String[]))
            throw new IllegalArgumentException("Script lambda types must have a String[] configuration object");
        if (type.equals(Type.CLASS) && !(configObject instanceof Class))
            throw new IllegalArgumentException("Class lambda types must have a Class configuration object");

        final LambdaHolder<T> lambdaHolder = new LambdaHolder<>();
        lambdaHolder.type = type;
        lambdaHolder.configKeyPrefix = configKeyPrefix;
        lambdaHolder.configObject = configObject;
        lambdaHolder.storeState(configuration);
        return lambdaHolder;
    }

    public static <T> LambdaHolder<T> loadState(final Configuration configuration, final String configKeyPrefix) {
        if (!configuration.containsKey(configKeyPrefix.concat(DOT_TYPE)))
            return null;
        if (!configuration.containsKey(configKeyPrefix.concat(DOT_OBJECT)))
            return null;

        final LambdaHolder<T> lambdaHolder = new LambdaHolder<>();
        lambdaHolder.configKeyPrefix = configKeyPrefix;
        lambdaHolder.type = Type.valueOf(configuration.getString(lambdaHolder.configKeyPrefix.concat(DOT_TYPE)));
        if (lambdaHolder.type.equals(Type.OBJECT)) {
            lambdaHolder.configObject = configuration.getProperty(lambdaHolder.configKeyPrefix.concat(DOT_OBJECT));
            lambdaHolder.realObject = lambdaHolder.configObject;
        } else if (lambdaHolder.type.equals(Type.CLASS)) {
            try {
                final Class klass = (Class) Class.forName(configuration.getString(lambdaHolder.configKeyPrefix.concat(DOT_OBJECT)));
                lambdaHolder.configObject = klass;
                lambdaHolder.realObject = klass.newInstance();
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        } else { // SCRIPT
            try {
                final String[] script = VertexProgramHelper.deserialize(configuration, lambdaHolder.configKeyPrefix.concat(DOT_OBJECT));
                lambdaHolder.configObject = script;
                lambdaHolder.realObject = new ScriptEngineLambda(script[0], script[1]);
            } catch (Exception e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
        return lambdaHolder;
    }


    public void storeState(final Configuration configuration) {
        configuration.setProperty(this.configKeyPrefix.concat(DOT_TYPE), this.type.name());
        if (this.type.equals(Type.OBJECT)) {
            configuration.setProperty(this.configKeyPrefix.concat(DOT_OBJECT), this.configObject);
        } else if (this.type.equals(Type.CLASS)) {
            configuration.setProperty(this.configKeyPrefix.concat(DOT_OBJECT), ((Class) this.configObject).getCanonicalName());
        } else { // SCRIPT
            try {
                VertexProgramHelper.serialize(this.configObject, configuration, this.configKeyPrefix.concat(DOT_OBJECT));
            } catch (Exception e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
    }


}
