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

namespace Gremlin.Net.Process.Traversal.Strategy.Decoration
{
    /// <summary>
    ///     OptionsStrategy makes no changes to the traversal itself - it just carries configuration information
    ///     at the traversal level.
    /// </summary>
    public class OptionsStrategy : AbstractTraversalStrategy
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="OptionsStrategy" /> class.
        /// </summary>
        public OptionsStrategy()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="OptionsStrategy" /> class.
        /// </summary>
        /// <param name="options">Specifies the options for the traversal.</param>
        public OptionsStrategy(IDictionary<string,object> options)
        {
            foreach(var item in options)
            {
                Configuration[item.Key] = item.Value;
            }
        }
    }
}