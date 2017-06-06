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
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Gremlin.Net.Process.Traversal.Strategy
{
    /// <summary>
    ///     Provides a common base class for strategies that are only included in <see cref="Bytecode" />
    ///     to be applied remotely.
    /// </summary>
    public abstract class AbstractTraversalStrategy : ITraversalStrategy, IEquatable<AbstractTraversalStrategy>
    {
        /// <summary>
        ///     Gets the name of the strategy.
        /// </summary>
        public string StrategyName => GetType().Name;

        /// <summary>
        ///     Gets the configuration of the strategy.
        /// </summary>
        public Dictionary<string, dynamic> Configuration { get; } = new Dictionary<string, dynamic>();

        /// <inheritdoc />
        public bool Equals(AbstractTraversalStrategy other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return StrategyName == other.StrategyName;
        }

        /// <inheritdoc />
        public virtual void Apply(ITraversal traversal)
        {
        }

        /// <inheritdoc />
        public virtual Task ApplyAsync(ITraversal traversal)
        {
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((AbstractTraversalStrategy) obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return StrategyName.GetHashCode();
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return StrategyName;
        }
    }
}