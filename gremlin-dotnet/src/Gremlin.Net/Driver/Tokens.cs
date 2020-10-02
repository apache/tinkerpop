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
using Gremlin.Net.Driver.Messages;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     String constants used to configure a <see cref="RequestMessage" />.
    /// </summary>
    public class Tokens
    {
        /// <summary>
        ///     The key for the unique identifier of the request. 
        /// </summary>
        public static string RequestId = "requestId";
        
        /// <summary>
        ///     Operation used by the client to authenticate itself.
        /// </summary>
        public static string OpsAuthentication = "authentication";

        /// <summary>
        ///     Operation used for a request that contains the Bytecode representation of a Traversal.
        /// </summary>
        public static string OpsBytecode = "bytecode";

        /// <summary>
        ///     Operation used to evaluate a Gremlin script provided as a string.
        /// </summary>
        public static string OpsEval = "eval";

        /// <summary>
        ///     Operation used to get a particular side-effect as produced by a previously executed Traversal.
        /// </summary>
        [Obsolete("As of release 3.3.8, not replaced, prefer use of cap()-step to retrieve side-effects as part of traversal iteration", false)]
        public static string OpsGather = "gather";

        /// <summary>
        ///     Operation used to get all the keys of all side-effects as produced by a previously executed Traversal.
        /// </summary>
        [Obsolete("As of release 3.3.8, not replaced, prefer use of cap()-step to retrieve side-effects as part of traversal iteration", false)]
        public static string OpsKeys = "keys";

        /// <summary>
        ///     Operation used to get all the keys of all side-effects as produced by a previously executed Traversal.
        /// </summary>
        public static string OpsClose = "close";

        /// <summary>
        ///     Default OpProcessor.
        /// </summary>
        public static string ProcessorTraversal = "traversal";

        /// <summary>
        ///     Session OpProcessor.
        /// </summary>
        public static string ProcessorSession = "session";

        /// <summary>
        ///     Argument name that allows to definition of the number of iterations each ResponseMessage should
        ///     contain - overrides the resultIterationBatchSize server setting.
        /// </summary>
        public static string ArgsBatchSize = "batchSize";

        /// <summary>
        ///     Argument name that allows definition of a map of key/value pairs to apply as variables in the
        ///     context of the Gremlin request sent to the server.
        /// </summary>
        public static string ArgsBindings = "bindings";

        /// <summary>
        ///     Argument name that allows definition of alias names for Graph and TraversalSource objects on the
        ///     remote system.
        /// </summary>
        public static string ArgsAliases = "aliases";

        /// <summary>
        ///     Argument name that corresponds to the Gremlin to evaluate.
        /// </summary>
        public static string ArgsGremlin = "gremlin";

        /// <summary>
        ///     Argument name that allows to define the id of session.
        /// </summary>
        public static string ArgsSession = "session";

        /// <summary>
        ///     Argument name that allows a value that is a custom string that the user can pass to a server that
        ///     might accept it for purpose of identifying the kind of client it came from.
        /// </summary>
        public static string ArgsUserAgent = "userAgent";

        /// <summary>
        ///     Argument name that allows to specify the unique identifier for the request.
        /// </summary>
        [Obsolete("As of release 3.3.8, not replaced, prefer use of cap()-step to retrieve side-effects as part of traversal iteration", false)]
        public static string ArgsSideEffect = "sideEffect";

        /// <summary>
        ///     Argument name that allows to specify the key for a specific side-effect.
        /// </summary>
        [Obsolete("As of release 3.3.8, not replaced, prefer use of cap()-step to retrieve side-effects as part of traversal iteration", false)]
        public static string ArgsSideEffectKey = "sideEffectKey";

        /// <summary>
        ///     <see cref="ResponseMessage{T}" /> argument that describes how side-effect data should be treated.
        /// </summary>
        [Obsolete("As of release 3.3.8, not replaced, prefer use of cap()-step to retrieve side-effects as part of traversal iteration", false)]
        public static string ArgsAggregateTo = "aggregateTo";

        /// <summary>
        ///     Argument name that allows definition of the flavor of Gremlin used (e.g. gremlin-groovy) to process the request.
        /// </summary>
        public static string ArgsLanguage = "language";

        /// <summary>
        ///     Argument name that allows the override of the server setting that determines the maximum time to wait for a
        ///     request to execute on the server.
        /// </summary>
        public static string ArgsEvalTimeout = "evaluationTimeout";

        /// <summary>
        ///     Argument name for the response to the server authentication challenge. This value is dependent on the SASL
        ///     authentication mechanism required by the server.
        /// </summary>
        public static string ArgsSasl = "sasl";

        [Obsolete("As of release 3.3.8, not replaced, prefer use of cap()-step to retrieve side-effects as part of traversal iteration", false)]
        internal static string ValAggregateToMap = "map";

        [Obsolete("As of release 3.3.8, not replaced, prefer use of cap()-step to retrieve side-effects as part of traversal iteration", false)]
        internal static string ValAggregateToBulkSet = "bulkset";
    }
}