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
using System.Net.WebSockets;

namespace Gremlin.Net.Driver.Exceptions
{
    /// <summary>
    ///     The exception that is thrown for requests that in-flight when the underlyingg websocket is closed by the server.
    ///     Requests that recieve this exception will be in a non-deteministic state and may need to be retried.
    /// </summary>
    /// <remarks>
    ///     It is recommended to use the same request retry policy that is applied when a <see cref="System.Net.WebSockets.WebSocketException" />,
    ///     <see cref="System.Net.Sockets.SocketException"/>, <see cref="System.IO.IOException" />.
    /// </remarks>
    public class ConnectionClosedException : Exception
    {
        /// <summary>
        ///     Gets the well-known WebSocket close status code provided by the server.
        /// </summary>
        public WebSocketCloseStatus? Status { get; }

        /// <summary>
        ///     Gets the Websocket closure description provided by the server.
        /// </summary>
        public string Description { get; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="ConnectionClosedException" /> class.
        /// </summary>
        public ConnectionClosedException(WebSocketCloseStatus? status, string description)
            : base(CreateMessage(status, description))
        {
            Status = status;
            Description = description;
        }

        private static string CreateMessage(WebSocketCloseStatus? status, string description)
        {
            return $"Connection closed by server. CloseStatus: {status?.ToString() ?? "<none>"}, CloseDescription: {description ?? "<none>"}." +
                " Any in-progress requests on the connection will be in an unknown state, and may need to be retried.";
        }
    }
}
