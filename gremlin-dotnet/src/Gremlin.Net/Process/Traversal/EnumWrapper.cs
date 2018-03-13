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

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     Represents an enum.
    /// </summary>
    public abstract class EnumWrapper
    {
        /// <summary>
        ///     Gets the name of the enum.
        /// </summary>
        public string EnumName { get; }

        /// <summary>
        ///     Gets the value of the enum.
        /// </summary>
        public string EnumValue { get; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="EnumWrapper" /> class.
        /// </summary>
        /// <param name="enumName">The name of the enum.</param>
        /// <param name="enumValue">The value of the enum.</param>
        protected EnumWrapper(string enumName, string enumValue)
        {
            EnumName = enumName;
            EnumValue = enumValue;
        }
    }
}