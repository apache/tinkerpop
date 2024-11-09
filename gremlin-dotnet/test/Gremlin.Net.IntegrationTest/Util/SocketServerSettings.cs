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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System;
using System.IO;
using YamlDotNet.Serialization;

namespace Gremlin.Net.IntegrationTest.Util;

public class SocketServerSettings
{
    [YamlMember(Alias = "PORT", ApplyNamingConventions = false)]
    public int Port { get; set; }

    /**
     * Configures which serializer will be used. Ex: "GraphBinaryV1" or "GraphSONV2"
     */
    [YamlMember(Alias = "SERIALIZER", ApplyNamingConventions = false)]
    public String Serializer { get; set; }

    /**
     * If a request with this ID comes to the server, the server responds back with a single vertex picked from Modern
     * graph.
     */
    [YamlMember(Alias = "SINGLE_VERTEX_REQUEST_ID", ApplyNamingConventions = false)]
    public Guid SingleVertexRequestId { get; set; }

    /**
     * If a request with this ID comes to the server, the server responds back with a single vertex picked from Modern
     * graph. After a 2 second delay, server sends a Close WebSocket frame on the same connection.
     */
    [YamlMember(Alias = "SINGLE_VERTEX_DELAYED_CLOSE_CONNECTION_REQUEST_ID", ApplyNamingConventions = false)]
    public Guid SingleVertexDelayedCloseConnectionRequestId { get; set; }

    /**
     * Server waits for 1 second, then responds with a 500 error status code
     */
    [YamlMember(Alias = "FAILED_AFTER_DELAY_REQUEST_ID", ApplyNamingConventions = false)]
    public Guid FailedAfterDelayRequestId { get; set; }

    /**
     * Server waits for 1 second then responds with a close web socket frame
     */
    [YamlMember(Alias = "CLOSE_CONNECTION_REQUEST_ID", ApplyNamingConventions = false)]
    public Guid CloseConnectionRequestId { get; set; }

    /**
     * Same as CLOSE_CONNECTION_REQUEST_ID
     */
    [YamlMember(Alias = "CLOSE_CONNECTION_REQUEST_ID_2", ApplyNamingConventions = false)]
    public Guid CloseConnectionRequestId2 { get; set; }

    /**
     * If a request with this ID comes to the server, the server responds with the user agent (if any) that was captured
     * during the web socket handshake.
     */
    [YamlMember(Alias = "USER_AGENT_REQUEST_ID", ApplyNamingConventions = false)]
    public Guid UserAgentRequestId { get; set; }
    
    /**
     * If a request with this ID comes to the server, the server responds with a string containing all overridden
     * per request settings from the request message. String will be of the form
     * "requestId=19436d9e-f8fc-4b67-8a76-deec60918424 evaluationTimeout=1234, batchSize=12, userAgent=testUserAgent"
     */
    [YamlMember(Alias = "PER_REQUEST_SETTINGS_REQUEST_ID", ApplyNamingConventions = false)]
    public Guid PerRequestSettingsRequestId { get; set; }
    
    public static SocketServerSettings FromYaml(String path)
    {
        var deserializer = new YamlDotNet.Serialization.DeserializerBuilder().IgnoreUnmatchedProperties().Build();

        return deserializer.Deserialize<SocketServerSettings>(File.ReadAllText(path));
    }
}