/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.gremlin.structure.io.ClassRegistry;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

/**
 * Resolves the name a {@code Class} value carries through {@link ClassRegistry} rather than through a class loader, so
 * the named class is never loaded. Registering this keeps Jackson's own {@code Class} deserializer from running, which
 * would resolve the name with {@code Class.forName(name, true, loader)} and initialize whatever it reached.
 */
final class ClassJacksonDeserializer extends StdDeserializer<Class> {

    ClassJacksonDeserializer() {
        super(Class.class);
    }

    @Override
    public Class deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
        // the parser arrives positioned on the value, so the name is the current token and not the next one.
        // A value that is not a string names no class, so it becomes a null name and is refused by the same lookup.
        final String name = JsonToken.VALUE_STRING == jsonParser.currentToken() ? jsonParser.getText() : null;

        return ClassRegistry.lookup(name).
                orElseThrow(() -> new IOException("Class not recognized - " + name));
    }

    @Override
    public boolean isCachable() {
        return true;
    }
}
