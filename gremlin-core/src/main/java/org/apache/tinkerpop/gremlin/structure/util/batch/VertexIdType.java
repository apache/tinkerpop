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
package org.apache.tinkerpop.gremlin.structure.util.batch;

import org.apache.tinkerpop.gremlin.structure.util.batch.cache.LongIDVertexCache;
import org.apache.tinkerpop.gremlin.structure.util.batch.cache.ObjectIDVertexCache;
import org.apache.tinkerpop.gremlin.structure.util.batch.cache.StringIDVertexCache;
import org.apache.tinkerpop.gremlin.structure.util.batch.cache.URLCompression;
import org.apache.tinkerpop.gremlin.structure.util.batch.cache.VertexCache;

/**
 * Type of vertex ids expected by BatchGraph. The default is IdType.OBJECT.
 * Use the IdType that best matches the used vertex id types in order to save sideEffects.
 *
 * @author Matthias Broecheler (http://www.matthiasb.com)
 */
public enum VertexIdType {

    OBJECT {
        @Override
        public VertexCache getVertexCache() {
            return new ObjectIDVertexCache();
        }
    },

    NUMBER {
        @Override
        public VertexCache getVertexCache() {
            return new LongIDVertexCache();
        }
    },

    STRING {
        @Override
        public VertexCache getVertexCache() {
            return new StringIDVertexCache();
        }
    },

    URL {
        @Override
        public VertexCache getVertexCache() {
            return new StringIDVertexCache(new URLCompression());

        }
    };

    public abstract VertexCache getVertexCache();
}
