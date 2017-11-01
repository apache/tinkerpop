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
    ///     Partitions the vertices, edges and vertex properties of a graph into String named partitions.
    /// </summary>
    public class PartitionStrategy : AbstractTraversalStrategy
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="PartitionStrategy" /> class.
        /// </summary>
        public PartitionStrategy()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="PartitionStrategy" /> class.
        /// </summary>
        /// <param name="partitionKey">Specifies the partition key name.</param>
        /// <param name="writePartition">
        ///     Specifies the name of the partition to write when adding vertices, edges and vertex
        ///     properties.
        /// </param>
        /// <param name="readPartitions">Specifies the partition of the graph to read from.</param>
        /// <param name="includeMetaProperties">Set to true if vertex properties should get assigned to partitions.</param>
        public PartitionStrategy(string partitionKey = null, string writePartition = null,
            IEnumerable<string> readPartitions = null, bool? includeMetaProperties = null)
        {
            if (partitionKey != null)
                Configuration["partitionKey"] = partitionKey;
            if (writePartition != null)
                Configuration["writePartition"] = writePartition;
            if (readPartitions != null)
                Configuration["readPartitions"] = readPartitions;
            if (includeMetaProperties != null)
                Configuration["includeMetaProperties"] = includeMetaProperties.Value;
        }
    }
}