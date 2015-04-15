/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.computer.util;


import org.apache.commons.configuration.Configuration;

import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LambdaHolder<S> implements Supplier<S> {

    public enum Type {
        OBJECT,
        SERIALIZED_OBJECT,
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
        } else if (lambdaHolder.type.equals(Type.SERIALIZED_OBJECT)) {
            lambdaHolder.configObject = VertexProgramHelper.deserialize(configuration, lambdaHolder.configKeyPrefix.concat(DOT_OBJECT));
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
            final String[] script = VertexProgramHelper.deserialize(configuration, lambdaHolder.configKeyPrefix.concat(DOT_OBJECT));
            lambdaHolder.configObject = script;
            lambdaHolder.realObject = new ScriptEngineLambda(script[0], script[1]);
        }
        return lambdaHolder;
    }


    public void storeState(final Configuration configuration) {
        configuration.setProperty(this.configKeyPrefix.concat(DOT_TYPE), this.type.name());
        if (this.type.equals(Type.OBJECT))
            configuration.setProperty(this.configKeyPrefix.concat(DOT_OBJECT), this.configObject);
        else if (this.type.equals(Type.SERIALIZED_OBJECT))
            VertexProgramHelper.serialize(this.configObject, configuration, this.configKeyPrefix.concat(DOT_OBJECT));
        else if (this.type.equals(Type.CLASS))
            configuration.setProperty(this.configKeyPrefix.concat(DOT_OBJECT), ((Class) this.configObject).getCanonicalName());
        else   // SCRIPT
            VertexProgramHelper.serialize(this.configObject, configuration, this.configKeyPrefix.concat(DOT_OBJECT));
    }


}
