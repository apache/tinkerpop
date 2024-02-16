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
package org.apache.tinkerpop.gremlin.util;

import org.apache.commons.configuration2.convert.DisabledListDelimiterHandler;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;

import java.util.Collections;

/**
 * Special handler that prevents the list delimiter from flattening iterable values that are used for configuration,
 * like a {@link Traversal} when given to {@link SubgraphStrategy}
 */
public class GremlinDisabledListDelimiterHandler extends DisabledListDelimiterHandler {

    private static final GremlinDisabledListDelimiterHandler INSTANCE = new GremlinDisabledListDelimiterHandler();

    public static GremlinDisabledListDelimiterHandler instance() {
        return INSTANCE;
    }

    @Override
    public Iterable<?> parse(final Object value) {
        if (value instanceof Iterable || value instanceof Traversal)
            return Collections.singletonList(value);
        else
            return super.parse(value);
    }
}
