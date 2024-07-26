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
package org.apache.tinkerpop.gremlin.util.message;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Data model for the "result" portion of a {@link ResponseMessageV4}.
 */
public final class ResponseResultV4 {
    private final BulkSet<Object> data;
    private final Map<String, Object> meta;

    public ResponseResultV4(final BulkSet<Object> data, final Map<String, Object> meta) {
        this.data = data;
        this.meta = meta;
    }

    public BulkSet<Object> getData() {
        return data;
    }

    public List<Object> getListData() {
        List results = new ArrayList(data.size());
        data.stream().forEach(res -> results.add(res));
        return results;
    }

    public Map<String, Object> getMeta() {
        return meta;
    }

    @Override
    public String toString() {
        return "ResponseResult{" +
                "data=" + data +
                ", meta=" + meta +
                '}';
    }
}
