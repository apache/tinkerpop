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
package org.apache.tinkerpop.gremlin.features;

import com.google.inject.Injector;
import io.cucumber.core.backend.ObjectFactory;
import io.cucumber.guice.ScenarioScope;

/**
 * Base class for implementing a custom {@code ObjectFactory} for cucumber tests and is based on the basic
 * {@code GuiceFactory} implementation which can't be extended. That implementation works well when there is only
 * single {@link World} implementation to test or if the requirements are such that system properties or environment
 * variables can otherwise suffice in controlling the test environment to the provider's satisfaction. In other cases,
 * it may simply be easier to create a custom {@code ObjectFactory} from this class, register it in
 * {@code META-INF/services/io.cucumber.core.backend.ObjectFactory} and then reference it directly in the
 * {@code CucumberOptions} annotation for the test class.
 */
public class AbstractGuiceFactory implements ObjectFactory {
    private final Injector injector;

    protected AbstractGuiceFactory(final Injector injector) {
        this.injector = injector;
    }

    public boolean addClass(final Class<?> clazz) {
        return true;
    }

    public void start() {
        injector.getInstance(ScenarioScope.class).enterScope();
    }

    public void stop() {
        injector.getInstance(ScenarioScope.class).exitScope();
    }

    public <T> T getInstance(final Class<T> clazz) {
        return injector.getInstance(clazz);
    }
}
