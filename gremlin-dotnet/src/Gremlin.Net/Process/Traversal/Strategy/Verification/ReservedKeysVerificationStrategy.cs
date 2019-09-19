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

using System.Collections.Generic;

namespace Gremlin.Net.Process.Traversal.Strategy.Verification
{
    /// <summary>
    ///     Provides a way to prevent traversal from using property keys that are reserved terms. By default, these
    ///     are "id" and "label" - providers may have their own reserved terms as well.
    /// </summary>
    public class ReservedKeysVerificationStrategy : AbstractTraversalStrategy
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="ReservedKeysVerificationStrategy" /> class.
        /// </summary>
        public ReservedKeysVerificationStrategy()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="ReservedKeysVerificationStrategy" /> class
        /// </summary>
        /// <param name="logWarning">Write a warning to the configured log on the server if a reserved key is used.</param>
        /// <param name="throwException">Throw an exception if a reserved key is used.</param>
        /// <param name="keys">List of keys to define as reserved. If not set then the defaults are used.</param>
        public ReservedKeysVerificationStrategy(bool logWarning = false, bool throwException = false, List<string> keys = null)
        {
            Configuration["logWarning"] = logWarning;
            Configuration["throwException"] = throwException;
            if (keys != null)
                Configuration["keys"] = keys;
        }
    }
}