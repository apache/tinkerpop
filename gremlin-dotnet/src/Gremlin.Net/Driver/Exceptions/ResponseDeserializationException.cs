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

using System;

namespace Gremlin.Net.Driver.Exceptions
{
    /// <summary>
    ///     The exception that is thrown when the driver fails to deserialize a response received from Gremlin Server.
    /// </summary>
    public class ResponseDeserializationException : Exception
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="ResponseDeserializationException" /> class.
        /// </summary>
        /// <param name="innerException">The exception that caused the deserialization failure.</param>
        public ResponseDeserializationException(Exception innerException)
            : base("Failed to deserialize the response received from Gremlin Server.", innerException)
        {
        }
    }
}
