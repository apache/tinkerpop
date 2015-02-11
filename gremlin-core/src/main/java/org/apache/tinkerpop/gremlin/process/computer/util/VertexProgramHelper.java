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
package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.util.Serializer;
import org.apache.commons.configuration.Configuration;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class VertexProgramHelper {

    private VertexProgramHelper() {
    }

    public static void serialize(final Object object, final Configuration configuration, final String key) {
        try {
            configuration.setProperty(key, Serializer.serializeObject(object));
        } catch (final IOException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    public static <T> T deserialize(final Configuration configuration, final String key) {
        try {
            final List byteList = configuration.getList(key);
            byte[] bytes = new byte[byteList.size()];
            for (int i = 0; i < byteList.size(); i++) {
                bytes[i] = Byte.valueOf(byteList.get(i).toString().replace("[", "").replace("]", ""));
            }
            return (T) Serializer.deserializeObject(bytes);
        } catch (final IOException | ClassNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    public static void verifyReversibility(final Traversal.Admin<?, ?> traversal) {
        if (!TraversalHelper.isReversible(traversal))
            throw new IllegalArgumentException("The provided traversal is not reversible");
    }

    public static void legalConfigurationKeyValueArray(final Object... configurationKeyValues) throws IllegalArgumentException {
        if (configurationKeyValues.length % 2 != 0)
            throw new IllegalArgumentException("The provided arguments must have a size that is a factor of 2");
        for (int i = 0; i < configurationKeyValues.length; i = i + 2) {
            if (!(configurationKeyValues[i] instanceof String))
                throw new IllegalArgumentException("The provided key/value array must have a String key on even array indices");
        }
    }
}
