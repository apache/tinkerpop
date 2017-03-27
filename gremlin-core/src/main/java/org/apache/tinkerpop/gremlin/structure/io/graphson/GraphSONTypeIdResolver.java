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

import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonTypeInfo;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.DatabindContext;
import org.apache.tinkerpop.shaded.jackson.databind.JavaType;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeIdResolver;
import org.apache.tinkerpop.shaded.jackson.databind.type.TypeFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Provides quick lookup for Type deserialization extracted from the JSON payload. As well as the Java Object to types
 * compatible for the version 2.0 of GraphSON.
 *
 * @author Kevin Gallardo (https://kgdo.me)
 */
public class GraphSONTypeIdResolver implements TypeIdResolver {

    private final Map<String, JavaType> idToType = new HashMap<>();

    private final Map<Class, String> typeToId = new HashMap<>();

    public Map<String, JavaType> getIdToType() {
        return idToType;
    }

    public Map<Class, String> getTypeToId() {
        return typeToId;
    }

    // Override manually a type definition.
    public GraphSONTypeIdResolver addCustomType(final String name, final Class clasz) {
        if (Tree.class.isAssignableFrom(clasz)) {
            // there is a special case for Tree which extends a Map, but has only 1 parametrized type,
            // and for which creating a default type is failing because it may fall into a
            // a self-referencing never-ending loop. Temporarily we force Tree<Element>
            // which should cover all the usage TinkerPop would do of the Trees anyway.
            getIdToType().put(name, TypeFactory.defaultInstance().constructType(new TypeReference<Tree<? extends Element>>() {}));
        } else {
            getIdToType().put(name, TypeFactory.defaultInstance().constructType(clasz));
        }
        getTypeToId().put(clasz, name);
        return this;
    }

    @Override
    public void init(final JavaType javaType) {
    }

    @Override
    public String idFromValue(final Object o) {
        return idFromValueAndType(o, o.getClass());
    }

    @Override
    public String idFromValueAndType(final Object o, final Class<?> aClass) {
        if (!getTypeToId().containsKey(aClass)) {
            // If one wants to serialize an object with a type, but hasn't registered
            // a typeID for that class, fail.
            throw new IllegalArgumentException(String.format("Could not find a type identifier for the class : %s. " +
                    "Make sure the value to serialize has a type identifier registered for its class.", aClass));
        } else {
            return getTypeToId().get(aClass);
        }
    }

    @Override
    public String idFromBaseType() {
        return null;
    }

    @Override
    public JavaType typeFromId(final DatabindContext databindContext, final String s) {
        // Get the type from the string from the stored Map. If not found, default to deserialize as a String.
        return getIdToType().containsKey(s)
                ? getIdToType().get(s)
                // TODO: shouldn't we fail instead, if the type is not found? Or log something?
                : TypeFactory.defaultInstance().constructType(String.class);
    }

    @Override
    public String getDescForKnownTypeIds() {
        // TODO: Not sure what to put here.
        return "GraphSON advanced typing system";
    }

    @Override
    public JsonTypeInfo.Id getMechanism() {
        return JsonTypeInfo.Id.CUSTOM;
    }
}
