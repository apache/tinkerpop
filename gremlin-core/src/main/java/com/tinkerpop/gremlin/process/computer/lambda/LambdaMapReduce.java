package com.tinkerpop.gremlin.process.computer.lambda;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.util.SupplierType;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.TriConsumer;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LambdaMapReduce<MK, MV, RK, RV, R> implements MapReduce<MK, MV, RK, RV, R> {

    public static final String LAMBDA_MAP_REDUCE_MEMORY_KEY = "gremlin.lambdaMapReduce.memoryKey";
    public static final String LAMBDA_MAP_REDUCE_MAP_LAMBDA = "gremlin.lambdaMapReduce.mapLambda";
    public static final String LAMBDA_MAP_REDUCE_COMBINE_LAMBDA = "gremlin.lambdaMapReduce.combineLambda";
    public static final String LAMBDA_MAP_REDUCE_REDUCE_LAMBDA = "gremlin.lambdaMapReduce.reduceLambda";
    public static final String LAMBDA_MAP_REDUCE_MEMORY_LAMBDA = "gremlin.lambdaMapReduce.memoryLambda";

    private static final String SUPPLIER_TYPE_KEY = "gremlin.lambdaMapReduce.supplierType";

    private SupplierType supplierType;

    private Pair<?, Supplier<BiConsumer<Vertex, MapEmitter<MK, MV>>>> supplierMapLambda;
    private BiConsumer<Vertex, MapEmitter<MK, MV>> mapLambda;

    private Pair<?, Supplier<TriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>>>> supplierCombineLambda;
    private TriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>> combineLambda;

    private Pair<?, Supplier<TriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>>>> supplierReduceLambda;
    private TriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>> reduceLambda;

    private Pair<?, Supplier<Function<Iterator<Pair<RK, RV>>, R>>> supplierMemoryLambda;
    private Function<Iterator<Pair<RK, RV>>, R> memoryLambda;

    private String sideEffectKey;

    private LambdaMapReduce() {

    }

    @Override
    public void loadState(final Configuration configuration) {
        this.supplierType = SupplierType.getType(configuration, SUPPLIER_TYPE_KEY);
        try {
            if (configuration.containsKey(LAMBDA_MAP_REDUCE_MAP_LAMBDA)) {
                this.supplierMapLambda = this.supplierType.<BiConsumer<Vertex, MapEmitter<MK, MV>>>get(configuration, LAMBDA_MAP_REDUCE_MAP_LAMBDA);
                this.mapLambda = this.supplierMapLambda.getValue1().get();
            }
            if (configuration.containsKey(LAMBDA_MAP_REDUCE_COMBINE_LAMBDA)) {
                this.supplierCombineLambda = this.supplierType.<TriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>>>get(configuration, LAMBDA_MAP_REDUCE_COMBINE_LAMBDA);
                this.combineLambda = this.supplierCombineLambda.getValue1().get();
            }
            if (configuration.containsKey(LAMBDA_MAP_REDUCE_REDUCE_LAMBDA)) {
                this.supplierReduceLambda = this.supplierType.<TriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>>>get(configuration, LAMBDA_MAP_REDUCE_REDUCE_LAMBDA);
                this.reduceLambda = this.supplierReduceLambda.getValue1().get();
            }
            if (configuration.containsKey(LAMBDA_MAP_REDUCE_MEMORY_LAMBDA)) {
                this.supplierMemoryLambda = this.supplierType.<Function<Iterator<Pair<RK, RV>>, R>>get(configuration, LAMBDA_MAP_REDUCE_MEMORY_LAMBDA);
                this.memoryLambda = this.supplierMemoryLambda.getValue1().get();
            } else {
                this.memoryLambda = s -> (R) s;
            }
            this.sideEffectKey = configuration.getString(LAMBDA_MAP_REDUCE_MEMORY_KEY, null);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void storeState(final Configuration configuration) {
        this.supplierType.set(configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_MAP_LAMBDA, this.supplierMapLambda.getValue0());
        this.supplierType.set(configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_COMBINE_LAMBDA, this.supplierCombineLambda.getValue0());
        this.supplierType.set(configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_REDUCE_LAMBDA, this.supplierReduceLambda.getValue0());
        this.supplierType.set(configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_MEMORY_LAMBDA, this.supplierMemoryLambda.getValue0());
        configuration.setProperty(LAMBDA_MAP_REDUCE_MEMORY_KEY, this.sideEffectKey);
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

        public Builder<MK, MV, RK, RV, R> map(final BiConsumer<Vertex, MapReduce.MapEmitter<MK, MV>> mapLambda) {
            SupplierType.OBJECT.set(this.configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_MAP_LAMBDA, (Supplier) () ->  mapLambda);
            return this;
        }

        public Builder<MK, MV, RK, RV, R> combine(TriConsumer<MK, Iterator<MV>, MapReduce.ReduceEmitter<RK, RV>> combineLambda) {
            SupplierType.OBJECT.set(this.configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_COMBINE_LAMBDA, (Supplier) () -> combineLambda);
            return this;
        }

        public Builder<MK, MV, RK, RV, R> reduce(TriConsumer<MK, Iterator<MV>, MapReduce.ReduceEmitter<RK, RV>> reduceLambda) {
            SupplierType.OBJECT.set(this.configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_REDUCE_LAMBDA, (Supplier) () -> reduceLambda);
            return this;
        }

        public Builder<MK, MV, RK, RV, R> memory(Function<Iterator<Pair<RK, RV>>, R> memoryLambda) {
            SupplierType.OBJECT.set(this.configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_MEMORY_LAMBDA, (Supplier) () -> memoryLambda);
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
