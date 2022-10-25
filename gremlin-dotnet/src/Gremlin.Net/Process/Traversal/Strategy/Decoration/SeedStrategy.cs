﻿#region License

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

namespace Gremlin.Net.Process.Traversal.Strategy.Decoration
{
    /// <summary>
    ///     A strategy that resets the specified {@code seed} value for Seedable steps like coin(), sample()
    ///     and Order.shuffle, which in turn will produce deterministic results from those steps.
    /// </summary>
    public class SeedStrategy : AbstractTraversalStrategy
    {
        private const string JavaFqcn = DecorationNamespace + nameof(SeedStrategy);
        
        /// <summary>
        ///     Initializes a new instance of the <see cref="SeedStrategy" /> class.
        /// </summary>
        public SeedStrategy() : base(JavaFqcn)
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="SeedStrategy" /> class.
        /// </summary>
        /// <param name="seed">Specifies the seed the traversal will use.</param>
        public SeedStrategy(long seed)
            : this()
        {
            Configuration["seed"] = seed;
        }
    }
}