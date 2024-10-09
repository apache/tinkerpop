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

import java.util.List;

/**
 * Data model for the "result" portion of a {@link ResponseMessage}.
 */
public final class ResponseResult {
    private final List<Object> data;
    private final boolean bulked;

    public ResponseResult(final List<Object> data, final boolean bulked) {
        this.bulked = bulked;
        this.data = data;
    }

    public List<Object> getData() {
        return data;
    }

    public boolean isBulked() {
        return bulked;
    }

    @Override
    public String toString() {
        return "ResponseResult{" +
                "data=" + data +
                '}';
    }
}
