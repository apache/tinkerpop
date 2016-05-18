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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;

import java.util.Map;

/**
 * A {@link SimpleModule} extension that does the necessary work to make the automatic typed deserialization
 * without full canonical class names work.
 *
 * Users of custom modules with the GraphSONMapper that want their objects to be deserialized automatically by the
 * mapper, must extend this class with their module. It is the only required step.
 *
 * Using this basis module allows the serialization and deserialization of typed objects without having the whole
 * canonical name of the serialized classes included in the Json payload. This is also necessary because Java
 * does not provide an agnostic way to search in a classpath a find a class by its simple name. Although that could
 * be done with an external library, later if we deem it necessary.
 *
 * @author Kevin Gallardo (https://kgdo.me)
 */
public abstract class TinkerPopJacksonModule extends SimpleModule {

    public TinkerPopJacksonModule(String name) {
        super(name);
    }

    public abstract Map<Class, String> getTypeDefinitions();

    public abstract String getTypeDomain();
}
