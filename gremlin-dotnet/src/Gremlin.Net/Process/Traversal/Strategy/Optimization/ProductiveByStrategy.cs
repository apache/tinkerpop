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

namespace Gremlin.Net.Process.Traversal.Strategy.Optimization
{
    /// <summary>
    ///     Ensures that all by() modulators end up producing a result with null as the default.
    /// </summary>
    public class ProductiveByStrategy : AbstractTraversalStrategy
    {
        private const string JavaFqcn = OptimizationNamespace + nameof(ProductiveByStrategy);
        
        /// <summary>
        ///     Initializes a new instance of the <see cref="ProductiveByStrategy" /> class.
        /// </summary>
        public ProductiveByStrategy() : base(JavaFqcn)
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="ProductiveByStrategy" /> class.
        /// </summary>
        /// <param name="productiveKeys">Specifies keys that will always be productive.</param>
        public ProductiveByStrategy(IEnumerable<object> productiveKeys = null)
            : this()
        {
            if (productiveKeys != null)
                Configuration["productiveKeys"] = productiveKeys;
        }
    }
}