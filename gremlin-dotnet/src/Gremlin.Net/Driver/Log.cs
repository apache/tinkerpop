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
        [LoggerMessage(20000, LogLevel.Information, "Initialized HTTP connections to {uri}")]
        public static partial void InitializedHttpConnection(this ILogger logger, Uri uri);
        
        [LoggerMessage(20001, LogLevel.Debug, "Submitting GremlinLang {gremlinLang}")]
        public static partial void SubmittingGremlinLang(this ILogger logger, GremlinLang gremlinLang);
    }
}