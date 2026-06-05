#region License

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

#endregion

namespace Gremlin.Net.IntegrationTest.Driver
{
    public static class SocketServerConstants
    {
        public const int Port = 45943;
        public const string GremlinSingleVertex = "server_single_vertex";
        public const string GremlinCloseConnection = "server_close_connection";
        public const string GremlinVertexThenClose = "server_vertex_then_close";
        public const string GremlinFailAfterDelay = "server_fail_after_delay";
        public const string GremlinPartialContentClose = "server_partial_content_close";
        public const string GremlinSlowResponse = "server_slow_response";
        public const string GremlinMalformedResponse = "server_malformed_response";
        public const string GremlinNoResponse = "server_no_response";
        public const string GremlinEmptyBody = "server_empty_body";
    }
}
