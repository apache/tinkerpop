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

/**
 * A verification harness for the JPMS boundaries used by a provider-supplied PDT extension. It is not an embedded
 * graph or remote-client example. The {@code provides} directive lets gremlin-core discover an adapter with
 * {@code ServiceLoader} without exporting its implementation package. The qualified {@code opens} directive lets
 * gremlin-core reflect over the private state and constructor of an annotated PDT type without opening the package
 * to every module. The application performs local GraphBinary round trips solely to execute both extension paths.
 */
module examples.jpms.extensions {
    requires org.apache.tinkerpop.gremlin.util;

    opens examples.jpms.extensions.types to org.apache.tinkerpop.gremlin.core;

    provides org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedTypeAdapter
        with examples.jpms.extensions.uint.Uint32Adapter;
}
