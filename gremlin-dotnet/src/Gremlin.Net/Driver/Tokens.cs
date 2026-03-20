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

using Gremlin.Net.Driver.Messages;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     String constants used to configure a <see cref="RequestMessage" />.
    /// </summary>
    public class Tokens
    {
        /// <summary>
        ///     Current TinkerPop version.
        /// </summary>
        public static string GremlinVersion = "4.0.0-SNAPSHOT"; // DO NOT MODIFY - Configured automatically by Maven Replacer Plugin

        /// <summary>
        ///     The key for the unique identifier of the request.
        /// </summary>
        public const string RequestId = "requestId";

        /// <summary>
        ///     Argument name that allows the definition of the number of items each ResponseMessage should
        ///     contain from a particular result - overrides the resultIterationBatchSize server setting.
        /// </summary>
        public const string ArgsBatchSize = "batchSize";

        /// <summary>
        ///     Argument name that allows definition of a map of key/value pairs to apply as variables in the
        ///     context of the Gremlin request sent to the server.
        /// </summary>
        public const string ArgsBindings = "bindings";

        /// <summary>
        ///     Argument name that corresponds to the Gremlin to evaluate.
        /// </summary>
        public const string ArgsGremlin = "gremlin";

        /// <summary>
        ///     Argument name that allows a value that is a custom string that the user can pass to a server that
        ///     might accept it for purpose of identifying the kind of client it came from.
        /// </summary>
        public const string ArgsUserAgent = "userAgent";

        /// <summary>
        ///     Argument name that allows definition of the flavor of Gremlin used (e.g. gremlin-groovy) to process the request.
        /// </summary>
        public const string ArgsLanguage = "language";

        /// <summary>
        ///     Argument name that allows the override of the server setting that determines the maximum time to wait for a
        ///     request to execute on the server.
        /// </summary>
        public const string ArgsEvalTimeout = "evaluationTimeout";

        /// <summary>
        ///     Argument name that allows the override of handling properties.
        ///     Allowed values: all, tokens
        /// </summary>
        public const string ArgMaterializeProperties = "materializeProperties";

        /// <summary>
        ///     Argument name for the traversal source alias.
        /// </summary>
        public const string ArgsG = "g";

        /// <summary>
        ///     Argument name for bulk results mode.
        /// </summary>
        public const string ArgsBulkResults = "bulkResults";
    }
}
