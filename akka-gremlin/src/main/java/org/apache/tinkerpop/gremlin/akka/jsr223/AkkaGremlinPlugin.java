/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.akka.jsr223;

import org.apache.tinkerpop.gremlin.akka.process.actor.AkkaGraphActors;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AkkaGremlinPlugin extends AbstractGremlinPlugin {

    protected static String NAME = "tinkerpop.akka";

    private static final AkkaGremlinPlugin INSTANCE = new AkkaGremlinPlugin();

    private static final ImportCustomizer imports = DefaultImportCustomizer.build().addClassImports(AkkaGraphActors.class).create();

    public AkkaGremlinPlugin() {
        super(NAME, imports);
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

    public static AkkaGremlinPlugin instance() {
        return INSTANCE;
    }
}