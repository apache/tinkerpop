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
using Gremlin.Net.Process.Traversal;
using Microsoft.Extensions.Logging;

namespace Gremlin.Net.Driver
{
    internal static partial class Log
    {
        [LoggerMessage(20000, LogLevel.Information, "Creating {nrConnections} new connections")]
        public static partial void FillingPool(this ILogger logger, int nrConnections);

        [LoggerMessage(20001, LogLevel.Warning,
            "Could not get a connection from the pool. Will try again. Retry {retryCount} of {maxRetries}")]
        public static partial void CouldNotGetConnectionFromPool(this ILogger logger, int retryCount, int maxRetries);

        [LoggerMessage(20002, LogLevel.Information,
            "A connection was closed. Removing it from the pool so it can be replaced.")]
        public static partial void RemovingClosedConnectionFromPool(this ILogger logger);
        
        [LoggerMessage(20003, LogLevel.Debug, "Submitting Bytecode {bytecode} for request: {requestId}")]
        public static partial void SubmittingBytecode(this ILogger logger, Bytecode bytecode, Guid requestId);
    }
}