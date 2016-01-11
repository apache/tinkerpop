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
package org.apache.tinkerpop.gremlin.groovy;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutorOverGraphTest;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineFileSandboxTest;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineOverGraphTest;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineSandboxCustomTest;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineSandboxedStandardTest;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineTinkerPopSandboxTest;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoaderTest;
import org.apache.tinkerpop.gremlin.groovy.plugin.dsl.credential.CredentialGraphTest;
import org.apache.tinkerpop.gremlin.groovy.util.SugarTestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The {@code GroovyEnvironmentSuite} is a JUnit test runner that executes the Gremlin Test Suite over a
 * {@link Graph} implementation.  This test suite covers ensures that a vendor implementation is compliant with
 * the Groovy "environment" which will typically ensure that the {@link Graph} will work as expected in the Gremlin
 * Console, Gremlin Server, and other Groovy environments.
 * <p/>
 * For more information on the usage of this suite, please see {@link StructureStandardSuite}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GroovyEnvironmentSuite extends AbstractGremlinSuite {

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] allTests = new Class<?>[]{
            CredentialGraphTest.class,
            GremlinGroovyScriptEngineOverGraphTest.class,
            GremlinGroovyScriptEngineSandboxCustomTest.class,
            GremlinGroovyScriptEngineSandboxedStandardTest.class,
            GremlinGroovyScriptEngineTinkerPopSandboxTest.class,
            GremlinGroovyScriptEngineFileSandboxTest.class,
            GremlinExecutorOverGraphTest.class,
            SugarLoaderTest.class,
    };

    public GroovyEnvironmentSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests, null, false, TraversalEngine.Type.STANDARD);
    }

    @Override
    public boolean beforeTestExecution(final Class<? extends AbstractGremlinTest> testClass) {
        unloadSugar();
        SugarLoader.load();
        return true;
    }

    @Override
    public void afterTestExecution(final Class<? extends AbstractGremlinTest> testClass) {
        unloadSugar();
    }

    private void unloadSugar() {
        try {
            SugarTestHelper.clearRegistry(GraphManager.getGraphProvider());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
