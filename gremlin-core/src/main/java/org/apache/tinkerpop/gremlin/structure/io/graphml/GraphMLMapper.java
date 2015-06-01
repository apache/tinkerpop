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
package org.apache.tinkerpop.gremlin.structure.io.graphml;

import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;

/**
 * This is an "empty" implementation only present for compatibility with {@link Io}.  GraphML is a "whole graph"
 * serialization format and does not have the notion of generic serialization functions.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GraphMLMapper implements Mapper<Object> {
    private GraphMLMapper() {}

    @Override
    public Object createMapper() {
        throw new UnsupportedOperationException("GraphML does not have an object mapper - it is a format for full Graph serialization");
    }

    public static Builder build() {
        return new Builder();
    }

    public final static class Builder implements Mapper.Builder<Builder> {
        @Override
        public Builder addRegistry(final IoRegistry registry) {
            throw new UnsupportedOperationException("GraphML does not accept custom serializers - it is a format for full Graph serialization");
        }

        public GraphMLMapper create() {
            return new GraphMLMapper();
        }
    }
}
