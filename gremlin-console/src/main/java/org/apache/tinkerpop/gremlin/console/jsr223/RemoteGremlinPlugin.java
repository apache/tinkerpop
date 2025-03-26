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
package org.apache.tinkerpop.gremlin.console.jsr223;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.auth.Auth;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RemoteGremlinPlugin extends AbstractGremlinPlugin {

    private static final String NAME = "tinkerpop.remote";

    private static final ImportCustomizer imports = DefaultImportCustomizer.build()
            .addClassImports(Cluster.class,
                    Client.class,
                    DriverRemoteConnection.class,
                    Auth.class)
            .addMethodImports(allStaticMethods(AnonymousTraversalSource.class))
            .addMethodImports(allStaticMethods(Auth.class))
            .create();

    public RemoteGremlinPlugin() {
        super(NAME, new HashSet<>(Collections.singletonList("gremlin-groovy")), imports);
    }

    private static List<Method> allStaticMethods(final Class<?> clazz) {
        return Stream.of(clazz.getMethods()).filter(m -> Modifier.isStatic(m.getModifiers())).collect(Collectors.toList());
    }
}
