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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.branch;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrOperatorFactory;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorRegistrar;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

import java.util.List;

/**
 * The operators for Exists, NotExists, And, Or, Local, Union, Coalesce, Optional, Choose, MapFirst and FlatMap, which
 * run their child plans as pipelines, and for {@link DegreeFilter}, a node of this package that filters vertices by
 * degree without a child.
 */
public final class BranchOperators implements OperatorRegistrar {

    @Override
    public void register(final CsrOperatorFactory factory) {
        factory.register(Ops.Exists.class, (node, spec) -> new ExistsOperator(node.plan(), false, spec));
        factory.register(Ops.NotExists.class, (node, spec) -> new ExistsOperator(node.plan(), true, spec));
        factory.register(Ops.And.class, (node, spec) -> new ConnectiveOperator(node.plans(), true, spec));
        factory.register(Ops.Or.class, (node, spec) -> new ConnectiveOperator(node.plans(), false, spec));
        factory.register(Ops.Local.class, (node, spec) ->
                new PerEntryOperator(PerEntryOperator.Mode.LOCAL, List.of(node.plan()), spec));
        factory.register(Ops.FlatMap.class, (node, spec) ->
                new PerEntryOperator(PerEntryOperator.Mode.FLAT_MAP, List.of(node.plan()), spec));
        factory.register(Ops.MapFirst.class, (node, spec) ->
                new PerEntryOperator(PerEntryOperator.Mode.MAP_FIRST, List.of(node.plan()), spec));
        factory.register(Ops.Coalesce.class, (node, spec) ->
                new PerEntryOperator(PerEntryOperator.Mode.COALESCE, node.plans(), spec));
        factory.register(Ops.Optional.class, (node, spec) ->
                new PerEntryOperator(PerEntryOperator.Mode.OPTIONAL, List.of(node.plan()), spec));
        factory.register(Ops.Union.class, (node, spec) -> new UnionOperator(node.plans(), spec));
        factory.register(Ops.Choose.class, (node, spec) -> new ChooseOperator(node, spec));
        factory.register(DegreeFilter.class, (node, spec) -> new DegreeFilterOperator(node, spec));
    }
}
