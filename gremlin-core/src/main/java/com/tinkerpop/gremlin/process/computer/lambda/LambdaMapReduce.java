package com.tinkerpop.gremlin.process.computer.lambda;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SBiConsumer;
import com.tinkerpop.gremlin.util.function.SFunction;
import com.tinkerpop.gremlin.util.function.STriConsumer;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LambdaMapReduce<MK, MV, RK, RV, R> implements MapReduce<MK, MV, RK, RV, R> {

    public static final String LAMBDA_MAP_REDUCE_MEMORY_KEY = "gremlin.lambdaMapReduce.memoryKey";
    public static final String LAMBDA_MAP_REDUCE_MAP_LAMBDA = "gremlin.lambdaMapReduce.mapLambda";
    public static final String LAMBDA_MAP_REDUCE_COMBINE_LAMBDA = "gremlin.lambdaMapReduce.combineLambda";
    public static final String LAMBDA_MAP_REDUCE_REDUCE_LAMBDA = "gremlin.lambdaMapReduce.reduceLambda";
    public static final String LAMBDA_MAP_REDUCE_MEMORY_LAMBDA = "gremlin.lambdaMapReduce.memoryLambda";


    private SBiConsumer<Vertex, MapEmitter<MK, MV>> mapLambda;
    private STriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>> combineLambda;
    private STriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>> reduceLambda;
    private SFunction<Iterator<Pair<RK, RV>>, R> memoryLambda;
    private String sideEffectKey;

    private LambdaMapReduce() {

    }

    @Override
    public void storeState(final Configuration configuration) {
        try {
            VertexProgramHelper.serialize(this.mapLambda, configuration, LAMBDA_MAP_REDUCE_MAP_LAMBDA);
            VertexProgramHelper.serialize(this.combineLambda, configuration, LAMBDA_MAP_REDUCE_COMBINE_LAMBDA);
            VertexProgramHelper.serialize(this.reduceLambda, configuration, LAMBDA_MAP_REDUCE_REDUCE_LAMBDA);
            VertexProgramHelper.serialize(this.sideEffectKey, configuration, LAMBDA_MAP_REDUCE_MEMORY_KEY);
            VertexProgramHelper.serialize(this.memoryLambda, configuration, LAMBDA_MAP_REDUCE_MEMORY_LAMBDA);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void loadState(final Configuration configuration) {
        try {
            this.mapLambda = configuration.containsKey(LAMBDA_MAP_REDUCE_MAP_LAMBDA) ?
                    VertexProgramHelper.deserialize(configuration, LAMBDA_MAP_REDUCE_MAP_LAMBDA) : null;
            this.combineLambda = configuration.containsKey(LAMBDA_MAP_REDUCE_COMBINE_LAMBDA) ?
                    VertexProgramHelper.deserialize(configuration, LAMBDA_MAP_REDUCE_COMBINE_LAMBDA) : null;
            this.reduceLambda = configuration.containsKey(LAMBDA_MAP_REDUCE_REDUCE_LAMBDA) ?
                    VertexProgramHelper.deserialize(configuration, LAMBDA_MAP_REDUCE_REDUCE_LAMBDA) : null;
            this.memoryLambda = configuration.containsKey(LAMBDA_MAP_REDUCE_MEMORY_LAMBDA) ?
                    VertexProgramHelper.deserialize(configuration, LAMBDA_MAP_REDUCE_MEMORY_LAMBDA) : s -> (R) s;
            this.sideEffectKey = configuration.getString(LAMBDA_MAP_REDUCE_MEMORY_KEY, null);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public boolean doStage(final Stage stage) {
        if (stage.equals(Stage.MAP))
            return null != this.mapLambda;
        else if (stage.equals(Stage.COMBINE))
            return null != this.combineLambda;
        else
            return null != this.reduceLambda;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<MK, MV> emitter) {
        this.mapLambda.accept(vertex, emitter);
    }

    @Override
    public void combine(final MK key, final Iterator<MV> values, final ReduceEmitter<RK, RV> emitter) {
        this.combineLambda.accept(key, values, emitter);
    }

    @Override
    public void reduce(final MK key, final Iterator<MV> values, final ReduceEmitter<RK, RV> emitter) {
        this.reduceLambda.accept(key, values, emitter);
    }

    @Override
    public R generateSideEffect(final Iterator<Pair<RK, RV>> keyValues) {
        return this.memoryLambda.apply(keyValues);
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    //////////////////

    public static <MK, MV, RK, RV, R> Builder<MK, MV, RK, RV, R> build() {
        return new Builder<MK, MV, RK, RV, R>();
    }

    public static class Builder<MK, MV, RK, RV, R> {

        private Configuration configuration = new BaseConfiguration();

        public Builder<MK, MV, RK, RV, R> map(final SBiConsumer<Vertex, MapReduce.MapEmitter<MK, MV>> mapLambda) {
            try {
                VertexProgramHelper.serialize(mapLambda, configuration, LambdaMapReduce.LAMBDA_MAP_REDUCE_MAP_LAMBDA);
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            return this;
        }

        public Builder<MK, MV, RK, RV, R> combine(STriConsumer<MK, Iterator<MV>, MapReduce.ReduceEmitter<RK, RV>> combineLambda) {
            try {
                VertexProgramHelper.serialize(combineLambda, configuration, LambdaMapReduce.LAMBDA_MAP_REDUCE_COMBINE_LAMBDA);
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            return this;
        }

        public Builder<MK, MV, RK, RV, R> reduce(STriConsumer<MK, Iterator<MV>, MapReduce.ReduceEmitter<RK, RV>> reduceLambda) {
            try {
                VertexProgramHelper.serialize(reduceLambda, configuration, LambdaMapReduce.LAMBDA_MAP_REDUCE_REDUCE_LAMBDA);
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            return this;
        }

        public Builder<MK, MV, RK, RV, R> memory(SFunction<Iterator<Pair<RK, RV>>, R> memoryLambda) {
            try {
                VertexProgramHelper.serialize(memoryLambda, configuration, LambdaMapReduce.LAMBDA_MAP_REDUCE_MEMORY_LAMBDA);
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            return this;
        }

        public Builder<MK, MV, RK, RV, R> memoryKey(final String memoryKey) {
            this.configuration.setProperty(LambdaMapReduce.LAMBDA_MAP_REDUCE_MEMORY_KEY, memoryKey);
            return this;
        }

        public LambdaMapReduce<MK, MV, RK, RV, R> create() {
            LambdaMapReduce<MK, MV, RK, RV, R> lambdaMapReduce = new LambdaMapReduce<>();
            lambdaMapReduce.loadState(this.configuration);
            return lambdaMapReduce;
        }
    }
}
