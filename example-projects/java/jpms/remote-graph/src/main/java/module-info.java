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
 * An application that gets {@code Graph} results from Gremlin Server. It only names the driver: TinkerGraph, which
 * holds deserialized {@code Graph} results, is bound as a {@code DefaultGraphProvider} service when its module is
 * on the module path, so the application doesn't need to require it.
 */
module examples.jpms.remotegraph {
    requires org.apache.tinkerpop.gremlin.driver;
}
