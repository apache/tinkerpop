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

namespace Gremlin.Net.Driver.Messages
{
    internal enum ResponseStatusCode
    {
        Success = 200,
        NoContent = 204,
        PartialContent = 206,
        Unauthorized = 401,
        Authenticate = 407,
        MalformedRequest = 498,
        InvalidRequestArguments = 499,
        ServerError = 500,
        ScriptEvaluationError = 597,
        ServerTimeout = 598,
        ServerSerializationError = 599
    }

    internal static class ResponseStatusCodeExtensions
    {
        public static bool IndicatesError(this ResponseStatusCode statusCode)
        {
            switch (statusCode)
            {
                case ResponseStatusCode.Success:
                case ResponseStatusCode.NoContent:
                case ResponseStatusCode.PartialContent:
                    return false;
                case ResponseStatusCode.Unauthorized:
                case ResponseStatusCode.Authenticate:
                case ResponseStatusCode.MalformedRequest:
                case ResponseStatusCode.InvalidRequestArguments:
                case ResponseStatusCode.ServerError:
                case ResponseStatusCode.ScriptEvaluationError:
                case ResponseStatusCode.ServerTimeout:
                case ResponseStatusCode.ServerSerializationError:
                    return true;
                default:
                    throw new ArgumentOutOfRangeException(nameof(statusCode), statusCode, null);
            }
        }
    }
}