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
package com.apache.tinkerpop.gremlin.driver.message;

import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResponseStatus {
    private final ResponseStatusCode code;
    private final String message;
    private final Map<String, Object> attributes;

    public ResponseStatus(final ResponseStatusCode code, final String message, final Map<String, Object> attributes) {
        this.code = code;
        this.message = message;
        this.attributes = attributes;
    }

    public ResponseStatusCode getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public String toString() {
        return "ResponseStatus{" +
                "code=" + code +
                ", message='" + message + '\'' +
                ", attributes=" + attributes +
                '}';
    }
}
