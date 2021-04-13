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
using System.Linq;

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     Represents a <see cref="Bytecode" /> instruction by an operator name and its arguments.
    /// </summary>
    public class Instruction : IEquatable<Instruction>
    {   
        /// <summary>
        ///     Initializes a new instance of the <see cref="Instruction" /> class.
        /// </summary>
        /// <param name="operatorName">The name of the operator.</param>
        /// <param name="arguments">The arguments.</param>
        public Instruction(string operatorName, params dynamic[] arguments)
        {
            OperatorName = operatorName;
            Arguments = arguments;
        }

        /// <summary>
        ///     Gets the name of the operator.
        /// </summary>
        public string OperatorName { get; }

        /// <summary>
        ///     Gets the arguments.
        /// </summary>
        public dynamic[] Arguments { get; }

        /// <summary>
        ///     String representation of the <see cref="Instruction"/>.
        /// </summary>
        public override string ToString()
        {
            return OperatorName + " [" + String.Join(",", Arguments) + "]";
        }

        /// <inheritdoc />
        public bool Equals(Instruction other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return OperatorName == other.OperatorName && Arguments.SequenceEqual(other.Arguments);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Instruction) obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                var hash = 19;
                if (OperatorName != null)
                {
                    hash = hash * 397 + OperatorName.GetHashCode();
                }

                if (Arguments != null)
                {
                    hash = Arguments.Aggregate(hash, (current, value) => current * 31 + value.GetHashCode());
                }

                return hash;
            }
        }
    }
}