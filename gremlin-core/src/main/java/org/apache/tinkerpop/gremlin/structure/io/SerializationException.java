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
package org.apache.tinkerpop.gremlin.structure.io;

import java.io.IOException;

/**
 * Indicates that a value could not be serialized or deserialized. As an {@link IOException} it can be handled by any
 * caller that already treats serialization failures as I/O failures, while allowing a caller that cares about the
 * distinction to identify a serialization problem specifically.
 */
public class SerializationException extends IOException {

    public SerializationException(final String message) {
        super(message);
    }

    public SerializationException(final Throwable cause) {
        super(cause);
    }

    public SerializationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
