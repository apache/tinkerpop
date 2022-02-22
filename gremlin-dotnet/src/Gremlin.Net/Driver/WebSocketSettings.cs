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

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Holds settings for WebSocket connections.
    /// </summary>
    internal class WebSocketSettings
    {
        // We currently only use this internally so we don't have to pass these values through the different layers
        // all the way down to the WebSocketConnection class. We can make this public however when we refactor the
        // configuration in general.
        
        /// <summary>
        ///     Gets or sets the delegate that will be invoked with the <see cref="ClientWebSocketOptions" />
        ///     object used to configure WebSocket connections.
        /// </summary>
        public Action<ClientWebSocketOptions> WebSocketConfigurationCallback { get; set; }
#if NET6_0_OR_GREATER
        /// <summary>
        ///     Gets or sets whether compressions will be used. The default is true. (Only available since .NET 6.)
        /// </summary>
        public bool UseCompression { get; set; } = true;

        /// <summary>
        ///     Gets or sets compression options. (Only available since .SET 6.)
        /// </summary>
        public WebSocketDeflateOptions CompressionOptions { get; set; } = new WebSocketDeflateOptions();
#endif
    }
}