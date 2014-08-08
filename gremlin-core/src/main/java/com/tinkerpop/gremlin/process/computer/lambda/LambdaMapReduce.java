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

    public static final String LAMBDA_MAP_REDUCE_SIDE_EFFECT_KEY = "gremlin.lambdaMapReduce.sideEffectKey";
    public static final String LAMBDA_MAP_REDUCE_MAP_LAMBDA = "gremlin.lambdaMapReduce.mapLambda";
    public static final String LAMBDA_MAP_REDUCE_COMBINE_LAMBDA = "gremlin.lambdaMapReduce.combineLambda";
    public static final String LAMBDA_MAP_REDUCE_REDUCE_LAMBDA = "gremlin.lambdaMapReduce.reduceLambda";
    public static final String LAMBDA_MAP_REDUCE_SIDE_EFFECT_LAMBDA = "gremlin.lambdaMapReduce.sideEffectLambda";


    private SBiConsumer<Vertex, MapEmitter<MK, MV>> mapLambda;
    private STriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>> combineLambda;
    private STriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>> reduceLambda;
    private SFunction<Iterator<Pair<RK, RV>>, R> sideEffectLambda;
    private String sideEffectKey;

    private LambdaMapReduce() {

    }

    public void storeState(final Configuration configuration) {
        try {
            VertexProgramHelper.serialize(this.mapLambda, configuration, LAMBDA_MAP_REDUCE_MAP_LAMBDA);
            VertexProgramHelper.serialize(this.combineLambda, configuration, LAMBDA_MAP_REDUCE_COMBINE_LAMBDA);
            VertexProgramHelper.serialize(this.reduceLambda, configuration, LAMBDA_MAP_REDUCE_REDUCE_LAMBDA);
            VertexProgramHelper.serialize(this.sideEffectKey, configuration, LAMBDA_MAP_REDUCE_SIDE_EFFECT_KEY);
            VertexProgramHelper.serialize(this.sideEffectLambda, configuration, LAMBDA_MAP_REDUCE_SIDE_EFFECT_LAMBDA);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void loadState(final Configuration configuration) {
        try {
            this.mapLambda = configuration.containsKey(LAMBDA_MAP_REDUCE_MAP_LAMBDA) ?
                    VertexProgramHelper.deserialize(configuration, LAMBDA_MAP_REDUCE_MAP_LAMBDA) : null;
            this.combineLambda = configuration.containsKey(LAMBDA_MAP_REDUCE_COMBINE_LAMBDA) ?
                    VertexProgramHelper.deserialize(configuration, LAMBDA_MAP_REDUCE_COMBINE_LAMBDA) : null;
            this.reduceLambda = configuration.containsKey(LAMBDA_MAP_REDUCE_REDUCE_LAMBDA) ?
                    VertexProgramHelper.deserialize(configuration, LAMBDA_MAP_REDUCE_REDUCE_LAMBDA) : null;
            this.sideEffectLambda = configuration.containsKey(LAMBDA_MAP_REDUCE_SIDE_EFFECT_LAMBDA) ?
                    VertexProgramHelper.deserialize(configuration, LAMBDA_MAP_REDUCE_SIDE_EFFECT_LAMBDA) : s -> (R) s;
            this.sideEffectKey = configuration.getString(LAMBDA_MAP_REDUCE_SIDE_EFFECT_KEY, null);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public boolean doStage(final Stage stage) {
        if (stage.equals(Stage.MAP))
            return null != this.mapLambda;
        else if (stage.equals(Stage.COMBINE))
            return null != this.combineLambda;
        else
            return null != this.reduceLambda;
    }

    public void map(final Vertex vertex, final MapEmitter<MK, MV> emitter) {
        this.mapLambda.accept(vertex, emitter);
    }

    public void combine(final MK key, final Iterator<MV> values, final ReduceEmitter<RK, RV> emitter) {
        this.combineLambda.accept(key, values, emitter);
    }

    public void reduce(final MK key, final Iterator<MV> values, final ReduceEmitter<RK, RV> emitter) {
        this.reduceLambda.accept(key, values, emitter);
    }

    public R generateSideEffect(final Iterator<Pair<RK, RV>> keyValues) {
        return this.sideEffectLambda.apply(keyValues);
    }

    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    //////////////////

    public static <MK, MV, RK, RV, R> Builder build() {
        return new Builder<MK, MV, RK, RV, R>();
    }

    public static class Builder<MK, MV, RK, RV, R> {

        private Configuration configuration = new BaseConfiguration();

        public Builder<MK, MV, RK, RV, R> map(final SBiConsumer<Vertex, MapEmitter<MK, MV>> mapLambda) {
            try {
                VertexProgramHelper.serialize(mapLambda, configuration, LAMBDA_MAP_REDUCE_MAP_LAMBDA);
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            return this;
        }

        public Builder<MK, MV, RK, RV, R> combine(STriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>> combineLambda) {
            try {
                VertexProgramHelper.serialize(combineLambda, configuration, LAMBDA_MAP_REDUCE_COMBINE_LAMBDA);
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            return this;
        }

        public Builder<MK, MV, RK, RV, R> reduce(STriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>> reduceLambda) {
            try {
                VertexProgramHelper.serialize(reduceLambda, configuration, LAMBDA_MAP_REDUCE_REDUCE_LAMBDA);
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            return this;
        }

        public Builder<MK, MV, RK, RV, R> sideEffect(SFunction<Iterator<Pair<RK, RV>>, R> sideEffectLambda) {
            try {
                VertexProgramHelper.serialize(sideEffectLambda, configuration, LAMBDA_MAP_REDUCE_SIDE_EFFECT_LAMBDA);
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            return this;
        }

        public Builder<MK, MV, RK, RV, R> sideEffectKey(final String sideEffectKey) {
            this.configuration.setProperty(LAMBDA_MAP_REDUCE_SIDE_EFFECT_KEY, sideEffectKey);
            return this;
        }

        public LambdaMapReduce create() {
            LambdaMapReduce lambdaMapReduce = new LambdaMapReduce();
            lambdaMapReduce.loadState(this.configuration);
            return lambdaMapReduce;
        }
    }
}
