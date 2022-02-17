/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.language.grammar;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ProductiveByStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.AbstractWarningVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EdgeLabelVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReservedKeysVerificationStrategy;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class TraversalStrategyVisitor extends DefaultGremlinBaseVisitor<TraversalStrategy> {
    protected final DefaultGremlinBaseVisitor<Traversal> tvisitor;

    public TraversalStrategyVisitor(final DefaultGremlinBaseVisitor<Traversal> tvisitor) {
        this.tvisitor = tvisitor;
    }

    @Override
    public TraversalStrategy visitTraversalStrategy(final GremlinParser.TraversalStrategyContext ctx) {
        // child count of one implies init syntax for the singleton constructed strategies. otherwise, it will
        // fall back to the Builder methods for construction
        if (ctx.getChildCount() == 1) {
            final String strategyName = ctx.getChild(0).getText();
            if (strategyName.equals(ReadOnlyStrategy.class.getSimpleName()))
                return ReadOnlyStrategy.instance();
            else if (strategyName.equals(ProductiveByStrategy.class.getSimpleName()))
                return ProductiveByStrategy.instance();
        } else if (ctx.getChild(0).getText().equals("new")) {
            final String strategyName = ctx.getChild(1).getText();
            if (strategyName.equals(PartitionStrategy.class.getSimpleName()))
                return getPartitionStrategy(ctx.traversalStrategyArgs_PartitionStrategy());
            else if (strategyName.equals(ReservedKeysVerificationStrategy.class.getSimpleName()))
                return getReservedKeysVerificationStrategy(ctx.traversalStrategyArgs_ReservedKeysVerificationStrategy());
            else if (strategyName.equals(EdgeLabelVerificationStrategy.class.getSimpleName()))
                return getEdgeLabelVerificationStrategy(ctx.traversalStrategyArgs_EdgeLabelVerificationStrategy());
            else if (strategyName.equals(SubgraphStrategy.class.getSimpleName()))
                return getSubgraphStrategy(ctx.traversalStrategyArgs_SubgraphStrategy());
            else if (strategyName.equals(SeedStrategy.class.getSimpleName()))
                return new SeedStrategy(Long.parseLong(ctx.integerLiteral().getText()));
            else if (strategyName.equals(ProductiveByStrategy.class.getSimpleName()))
                return getProductiveByStrategy(ctx.traversalStrategyArgs_ProductiveByStrategy());
        }
        throw new IllegalStateException("Unexpected TraversalStrategy specification - " + ctx.getText());
    }

    private static EdgeLabelVerificationStrategy getEdgeLabelVerificationStrategy(final List<GremlinParser.TraversalStrategyArgs_EdgeLabelVerificationStrategyContext> ctxs) {
        if (null == ctxs || ctxs.isEmpty())
            return EdgeLabelVerificationStrategy.build().create();

        final EdgeLabelVerificationStrategy.Builder builder = EdgeLabelVerificationStrategy.build();
        ctxs.forEach(ctx -> {
            switch (ctx.getChild(0).getText()) {
                case AbstractWarningVerificationStrategy.LOG_WARNING:
                    builder.logWarning(GenericLiteralVisitor.getBooleanLiteral(ctx.booleanLiteral()));
                    break;
                case AbstractWarningVerificationStrategy.THROW_EXCEPTION:
                    builder.throwException(GenericLiteralVisitor.getBooleanLiteral(ctx.booleanLiteral()));
                    break;
            }
        });

        return builder.create();
    }

    private static ReservedKeysVerificationStrategy getReservedKeysVerificationStrategy(final List<GremlinParser.TraversalStrategyArgs_ReservedKeysVerificationStrategyContext> ctxs) {
        if (null == ctxs || ctxs.isEmpty())
            return ReservedKeysVerificationStrategy.build().create();

        final ReservedKeysVerificationStrategy.Builder builder = ReservedKeysVerificationStrategy.build();
        ctxs.forEach(ctx -> {
            switch (ctx.getChild(0).getText()) {
                case AbstractWarningVerificationStrategy.LOG_WARNING:
                    builder.logWarning(GenericLiteralVisitor.getBooleanLiteral(ctx.booleanLiteral()));
                    break;
                case AbstractWarningVerificationStrategy.THROW_EXCEPTION:
                    builder.throwException(GenericLiteralVisitor.getBooleanLiteral(ctx.booleanLiteral()));
                    break;
                case ReservedKeysVerificationStrategy.KEYS:
                    builder.reservedKeys(new HashSet<>(Arrays.asList(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()))));
                    break;
            }
        });

        return builder.create();
    }

    private static PartitionStrategy getPartitionStrategy(final List<GremlinParser.TraversalStrategyArgs_PartitionStrategyContext> ctxs) {
        final PartitionStrategy.Builder builder = PartitionStrategy.build();
        ctxs.forEach(ctx -> {
            switch (ctx.getChild(0).getText()) {
                case PartitionStrategy.INCLUDE_META_PROPERTIES:
                    builder.includeMetaProperties(GenericLiteralVisitor.getBooleanLiteral(ctx.booleanLiteral()));
                    break;
                case PartitionStrategy.READ_PARTITIONS:
                    builder.readPartitions(Arrays.asList(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList())));
                    break;
                case PartitionStrategy.WRITE_PARTITION:
                    builder.writePartition(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
                    break;
                case PartitionStrategy.PARTITION_KEY:
                    builder.partitionKey(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
                    break;
            }
        });

        return builder.create();
    }

    private SubgraphStrategy getSubgraphStrategy(final List<GremlinParser.TraversalStrategyArgs_SubgraphStrategyContext> ctxs) {
        final SubgraphStrategy.Builder builder = SubgraphStrategy.build();
        ctxs.forEach(ctx -> {
            switch (ctx.getChild(0).getText()) {
                case SubgraphStrategy.VERTICES:
                    builder.vertices(tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
                    break;
                case SubgraphStrategy.EDGES:
                    builder.edges(tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
                    break;
                case SubgraphStrategy.VERTEX_PROPERTIES:
                    builder.vertexProperties(tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
                    break;
                case SubgraphStrategy.CHECK_ADJACENT_VERTICES:
                    builder.checkAdjacentVertices(GenericLiteralVisitor.getBooleanLiteral(ctx.booleanLiteral()));
                    break;
            }
        });

        return builder.create();
    }

    private static ProductiveByStrategy getProductiveByStrategy(final GremlinParser.TraversalStrategyArgs_ProductiveByStrategyContext ctx) {
        final ProductiveByStrategy.Builder builder = ProductiveByStrategy.build();
        builder.productiveKeys(Arrays.asList(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList())));
        return builder.create();
    }
}
