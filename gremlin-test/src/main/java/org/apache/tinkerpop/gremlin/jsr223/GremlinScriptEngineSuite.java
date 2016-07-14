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
package org.apache.tinkerpop.gremlin.jsr223;

import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinScriptEngineSuite extends Suite {
    static String ENGINE_TO_TEST;

    private static final Class<?>[] allTests = new Class<?>[]{
            CachedGremlinScriptEngineManagerTest.class,
            GremlinEnabledScriptEngineTest.class,
            ScriptEngineLambdaTest.class };

    public GremlinScriptEngineSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(builder, klass, allTests);
        ENGINE_TO_TEST = getScriptEngineName(klass);
    }

    public static String getScriptEngineName(final Class<?> klass) throws InitializationError {
        final ScriptEngineToTest annotation = klass.getAnnotation(ScriptEngineToTest.class);
        if (null == annotation)
            throw new InitializationError(String.format("class '%s' must have a EngineToTest annotation", klass.getName()));
        return annotation.scriptEngineName();
    }
}
