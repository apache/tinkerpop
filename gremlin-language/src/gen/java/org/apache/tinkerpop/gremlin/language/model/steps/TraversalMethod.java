package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public abstract class TraversalMethod {
    private TraversalMethod() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a TraversalMethod according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(V instance) ;
        
        R visit(AddE instance) ;
        
        R visit(AddV instance) ;
        
        R visit(Aggregate instance) ;
        
        R visit(And instance) ;
        
        R visit(As instance) ;
        
        R visit(Barrier instance) ;
        
        R visit(Both instance) ;
        
        R visit(BothE instance) ;
        
        R visit(BothV instance) ;
        
        R visit(Branch instance) ;
        
        R visit(By instance) ;
        
        R visit(Cap instance) ;
        
        R visit(Choose instance) ;
        
        R visit(Coalesce instance) ;
        
        R visit(Coin instance) ;
        
        R visit(ConnectedComponent instance) ;
        
        R visit(Constant instance) ;
        
        R visit(Count instance) ;
        
        R visit(CyclicPath instance) ;
        
        R visit(Dedup instance) ;
        
        R visit(Drop instance) ;
        
        R visit(ElementMap instance) ;
        
        R visit(Emit instance) ;
        
        R visit(Filter instance) ;
        
        R visit(FlatMap instance) ;
        
        R visit(Fold instance) ;
        
        R visit(From instance) ;
        
        R visit(Group instance) ;
        
        R visit(GroupCount instance) ;
        
        R visit(Has instance) ;
        
        R visit(HasId instance) ;
        
        R visit(HasKey instance) ;
        
        R visit(HasLabel instance) ;
        
        R visit(HasNot instance) ;
        
        R visit(HasValue instance) ;
        
        R visit(Id instance) ;
        
        R visit(Identity instance) ;
        
        R visit(In instance) ;
        
        R visit(InE instance) ;
        
        R visit(InV instance) ;
        
        R visit(Index instance) ;
        
        R visit(Inject instance) ;
        
        R visit(Is instance) ;
        
        R visit(Key instance) ;
        
        R visit(Label instance) ;
        
        R visit(Limit instance) ;
        
        R visit(Local instance) ;
        
        R visit(Loops instance) ;
        
        R visit(Map instance) ;
        
        R visit(Match instance) ;
        
        R visit(MathEsc instance) ;
        
        R visit(Max instance) ;
        
        R visit(Mean instance) ;
        
        R visit(Min instance) ;
        
        R visit(Not instance) ;
        
        R visit(Option instance) ;
        
        R visit(Optional instance) ;
        
        R visit(Or instance) ;
        
        R visit(Order instance) ;
        
        R visit(OtherV instance) ;
        
        R visit(Out instance) ;
        
        R visit(OutE instance) ;
        
        R visit(OutV instance) ;
        
        R visit(PageRank instance) ;
        
        R visit(Path instance) ;
        
        R visit(PeerPressure instance) ;
        
        R visit(Profile instance) ;
        
        R visit(Project instance) ;
        
        R visit(Properties instance) ;
        
        R visit(Property instance) ;
        
        R visit(PropertyMap instance) ;
        
        R visit(Range instance) ;
        
        R visit(Read instance) ;
        
        R visit(Repeat instance) ;
        
        R visit(Sack instance) ;
        
        R visit(Sample instance) ;
        
        R visit(Select instance) ;
        
        R visit(ShortestPath instance) ;
        
        R visit(SideEffect instance) ;
        
        R visit(SimplePath instance) ;
        
        R visit(Skip instance) ;
        
        R visit(Store instance) ;
        
        R visit(Subgraph instance) ;
        
        R visit(Sum instance) ;
        
        R visit(Tail instance) ;
        
        R visit(TimeLimit instance) ;
        
        R visit(Times instance) ;
        
        R visit(To instance) ;
        
        R visit(ToE instance) ;
        
        R visit(ToV instance) ;
        
        R visit(Tree instance) ;
        
        R visit(Unfold instance) ;
        
        R visit(Union instance) ;
        
        R visit(Until instance) ;
        
        R visit(Value instance) ;
        
        R visit(ValueMap instance) ;
        
        R visit(Values instance) ;
        
        R visit(Where instance) ;
        
        R visit(With instance) ;
        
        R visit(Write instance) ;
    }
    
    /**
     * An interface for applying a function to a TraversalMethod according to its variant (subclass). If a visit() method
     * for a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(TraversalMethod instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(V instance) {
            return otherwise(instance);
        }
        
        default R visit(AddE instance) {
            return otherwise(instance);
        }
        
        default R visit(AddV instance) {
            return otherwise(instance);
        }
        
        default R visit(Aggregate instance) {
            return otherwise(instance);
        }
        
        default R visit(And instance) {
            return otherwise(instance);
        }
        
        default R visit(As instance) {
            return otherwise(instance);
        }
        
        default R visit(Barrier instance) {
            return otherwise(instance);
        }
        
        default R visit(Both instance) {
            return otherwise(instance);
        }
        
        default R visit(BothE instance) {
            return otherwise(instance);
        }
        
        default R visit(BothV instance) {
            return otherwise(instance);
        }
        
        default R visit(Branch instance) {
            return otherwise(instance);
        }
        
        default R visit(By instance) {
            return otherwise(instance);
        }
        
        default R visit(Cap instance) {
            return otherwise(instance);
        }
        
        default R visit(Choose instance) {
            return otherwise(instance);
        }
        
        default R visit(Coalesce instance) {
            return otherwise(instance);
        }
        
        default R visit(Coin instance) {
            return otherwise(instance);
        }
        
        default R visit(ConnectedComponent instance) {
            return otherwise(instance);
        }
        
        default R visit(Constant instance) {
            return otherwise(instance);
        }
        
        default R visit(Count instance) {
            return otherwise(instance);
        }
        
        default R visit(CyclicPath instance) {
            return otherwise(instance);
        }
        
        default R visit(Dedup instance) {
            return otherwise(instance);
        }
        
        default R visit(Drop instance) {
            return otherwise(instance);
        }
        
        default R visit(ElementMap instance) {
            return otherwise(instance);
        }
        
        default R visit(Emit instance) {
            return otherwise(instance);
        }
        
        default R visit(Filter instance) {
            return otherwise(instance);
        }
        
        default R visit(FlatMap instance) {
            return otherwise(instance);
        }
        
        default R visit(Fold instance) {
            return otherwise(instance);
        }
        
        default R visit(From instance) {
            return otherwise(instance);
        }
        
        default R visit(Group instance) {
            return otherwise(instance);
        }
        
        default R visit(GroupCount instance) {
            return otherwise(instance);
        }
        
        default R visit(Has instance) {
            return otherwise(instance);
        }
        
        default R visit(HasId instance) {
            return otherwise(instance);
        }
        
        default R visit(HasKey instance) {
            return otherwise(instance);
        }
        
        default R visit(HasLabel instance) {
            return otherwise(instance);
        }
        
        default R visit(HasNot instance) {
            return otherwise(instance);
        }
        
        default R visit(HasValue instance) {
            return otherwise(instance);
        }
        
        default R visit(Id instance) {
            return otherwise(instance);
        }
        
        default R visit(Identity instance) {
            return otherwise(instance);
        }
        
        default R visit(In instance) {
            return otherwise(instance);
        }
        
        default R visit(InE instance) {
            return otherwise(instance);
        }
        
        default R visit(InV instance) {
            return otherwise(instance);
        }
        
        default R visit(Index instance) {
            return otherwise(instance);
        }
        
        default R visit(Inject instance) {
            return otherwise(instance);
        }
        
        default R visit(Is instance) {
            return otherwise(instance);
        }
        
        default R visit(Key instance) {
            return otherwise(instance);
        }
        
        default R visit(Label instance) {
            return otherwise(instance);
        }
        
        default R visit(Limit instance) {
            return otherwise(instance);
        }
        
        default R visit(Local instance) {
            return otherwise(instance);
        }
        
        default R visit(Loops instance) {
            return otherwise(instance);
        }
        
        default R visit(Map instance) {
            return otherwise(instance);
        }
        
        default R visit(Match instance) {
            return otherwise(instance);
        }
        
        default R visit(MathEsc instance) {
            return otherwise(instance);
        }
        
        default R visit(Max instance) {
            return otherwise(instance);
        }
        
        default R visit(Mean instance) {
            return otherwise(instance);
        }
        
        default R visit(Min instance) {
            return otherwise(instance);
        }
        
        default R visit(Not instance) {
            return otherwise(instance);
        }
        
        default R visit(Option instance) {
            return otherwise(instance);
        }
        
        default R visit(Optional instance) {
            return otherwise(instance);
        }
        
        default R visit(Or instance) {
            return otherwise(instance);
        }
        
        default R visit(Order instance) {
            return otherwise(instance);
        }
        
        default R visit(OtherV instance) {
            return otherwise(instance);
        }
        
        default R visit(Out instance) {
            return otherwise(instance);
        }
        
        default R visit(OutE instance) {
            return otherwise(instance);
        }
        
        default R visit(OutV instance) {
            return otherwise(instance);
        }
        
        default R visit(PageRank instance) {
            return otherwise(instance);
        }
        
        default R visit(Path instance) {
            return otherwise(instance);
        }
        
        default R visit(PeerPressure instance) {
            return otherwise(instance);
        }
        
        default R visit(Profile instance) {
            return otherwise(instance);
        }
        
        default R visit(Project instance) {
            return otherwise(instance);
        }
        
        default R visit(Properties instance) {
            return otherwise(instance);
        }
        
        default R visit(Property instance) {
            return otherwise(instance);
        }
        
        default R visit(PropertyMap instance) {
            return otherwise(instance);
        }
        
        default R visit(Range instance) {
            return otherwise(instance);
        }
        
        default R visit(Read instance) {
            return otherwise(instance);
        }
        
        default R visit(Repeat instance) {
            return otherwise(instance);
        }
        
        default R visit(Sack instance) {
            return otherwise(instance);
        }
        
        default R visit(Sample instance) {
            return otherwise(instance);
        }
        
        default R visit(Select instance) {
            return otherwise(instance);
        }
        
        default R visit(ShortestPath instance) {
            return otherwise(instance);
        }
        
        default R visit(SideEffect instance) {
            return otherwise(instance);
        }
        
        default R visit(SimplePath instance) {
            return otherwise(instance);
        }
        
        default R visit(Skip instance) {
            return otherwise(instance);
        }
        
        default R visit(Store instance) {
            return otherwise(instance);
        }
        
        default R visit(Subgraph instance) {
            return otherwise(instance);
        }
        
        default R visit(Sum instance) {
            return otherwise(instance);
        }
        
        default R visit(Tail instance) {
            return otherwise(instance);
        }
        
        default R visit(TimeLimit instance) {
            return otherwise(instance);
        }
        
        default R visit(Times instance) {
            return otherwise(instance);
        }
        
        default R visit(To instance) {
            return otherwise(instance);
        }
        
        default R visit(ToE instance) {
            return otherwise(instance);
        }
        
        default R visit(ToV instance) {
            return otherwise(instance);
        }
        
        default R visit(Tree instance) {
            return otherwise(instance);
        }
        
        default R visit(Unfold instance) {
            return otherwise(instance);
        }
        
        default R visit(Union instance) {
            return otherwise(instance);
        }
        
        default R visit(Until instance) {
            return otherwise(instance);
        }
        
        default R visit(Value instance) {
            return otherwise(instance);
        }
        
        default R visit(ValueMap instance) {
            return otherwise(instance);
        }
        
        default R visit(Values instance) {
            return otherwise(instance);
        }
        
        default R visit(Where instance) {
            return otherwise(instance);
        }
        
        default R visit(With instance) {
            return otherwise(instance);
        }
        
        default R visit(Write instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.VStep
     */
    public static final class V extends TraversalMethod {
        public final VStep v;
        
        /**
         * Constructs an immutable V object
         */
        public V(VStep v) {
            this.v = v;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof V)) return false;
            V o = (V) other;
            return v.equals(o.v);
        }
        
        @Override
        public int hashCode() {
            return 2 * v.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.AddEStep
     */
    public static final class AddE extends TraversalMethod {
        public final AddEStep addE;
        
        /**
         * Constructs an immutable AddE object
         */
        public AddE(AddEStep addE) {
            this.addE = addE;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof AddE)) return false;
            AddE o = (AddE) other;
            return addE.equals(o.addE);
        }
        
        @Override
        public int hashCode() {
            return 2 * addE.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.AddVStep
     */
    public static final class AddV extends TraversalMethod {
        public final AddVStep addV;
        
        /**
         * Constructs an immutable AddV object
         */
        public AddV(AddVStep addV) {
            this.addV = addV;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof AddV)) return false;
            AddV o = (AddV) other;
            return addV.equals(o.addV);
        }
        
        @Override
        public int hashCode() {
            return 2 * addV.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.AggregateStep
     */
    public static final class Aggregate extends TraversalMethod {
        public final AggregateStep aggregate;
        
        /**
         * Constructs an immutable Aggregate object
         */
        public Aggregate(AggregateStep aggregate) {
            this.aggregate = aggregate;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Aggregate)) return false;
            Aggregate o = (Aggregate) other;
            return aggregate.equals(o.aggregate);
        }
        
        @Override
        public int hashCode() {
            return 2 * aggregate.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.AndStep
     */
    public static final class And extends TraversalMethod {
        public final AndStep and;
        
        /**
         * Constructs an immutable And object
         */
        public And(AndStep and) {
            this.and = and;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof And)) return false;
            And o = (And) other;
            return and.equals(o.and);
        }
        
        @Override
        public int hashCode() {
            return 2 * and.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.AsStep
     */
    public static final class As extends TraversalMethod {
        public final AsStep as;
        
        /**
         * Constructs an immutable As object
         */
        public As(AsStep as) {
            this.as = as;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof As)) return false;
            As o = (As) other;
            return as.equals(o.as);
        }
        
        @Override
        public int hashCode() {
            return 2 * as.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.BarrierStep
     */
    public static final class Barrier extends TraversalMethod {
        public final BarrierStep barrier;
        
        /**
         * Constructs an immutable Barrier object
         */
        public Barrier(BarrierStep barrier) {
            this.barrier = barrier;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Barrier)) return false;
            Barrier o = (Barrier) other;
            return barrier.equals(o.barrier);
        }
        
        @Override
        public int hashCode() {
            return 2 * barrier.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.BothStep
     */
    public static final class Both extends TraversalMethod {
        public final BothStep both;
        
        /**
         * Constructs an immutable Both object
         */
        public Both(BothStep both) {
            this.both = both;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Both)) return false;
            Both o = (Both) other;
            return both.equals(o.both);
        }
        
        @Override
        public int hashCode() {
            return 2 * both.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.BothEStep
     */
    public static final class BothE extends TraversalMethod {
        public final BothEStep bothE;
        
        /**
         * Constructs an immutable BothE object
         */
        public BothE(BothEStep bothE) {
            this.bothE = bothE;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof BothE)) return false;
            BothE o = (BothE) other;
            return bothE.equals(o.bothE);
        }
        
        @Override
        public int hashCode() {
            return 2 * bothE.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.BothVStep
     */
    public static final class BothV extends TraversalMethod {
        public final BothVStep bothV;
        
        /**
         * Constructs an immutable BothV object
         */
        public BothV(BothVStep bothV) {
            this.bothV = bothV;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof BothV)) return false;
            BothV o = (BothV) other;
            return bothV.equals(o.bothV);
        }
        
        @Override
        public int hashCode() {
            return 2 * bothV.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.BranchStep
     */
    public static final class Branch extends TraversalMethod {
        public final BranchStep branch;
        
        /**
         * Constructs an immutable Branch object
         */
        public Branch(BranchStep branch) {
            this.branch = branch;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Branch)) return false;
            Branch o = (Branch) other;
            return branch.equals(o.branch);
        }
        
        @Override
        public int hashCode() {
            return 2 * branch.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.ByStep
     */
    public static final class By extends TraversalMethod {
        public final ByStep by;
        
        /**
         * Constructs an immutable By object
         */
        public By(ByStep by) {
            this.by = by;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof By)) return false;
            By o = (By) other;
            return by.equals(o.by);
        }
        
        @Override
        public int hashCode() {
            return 2 * by.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.CapStep
     */
    public static final class Cap extends TraversalMethod {
        public final CapStep cap;
        
        /**
         * Constructs an immutable Cap object
         */
        public Cap(CapStep cap) {
            this.cap = cap;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Cap)) return false;
            Cap o = (Cap) other;
            return cap.equals(o.cap);
        }
        
        @Override
        public int hashCode() {
            return 2 * cap.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.ChooseStep
     */
    public static final class Choose extends TraversalMethod {
        public final ChooseStep choose;
        
        /**
         * Constructs an immutable Choose object
         */
        public Choose(ChooseStep choose) {
            this.choose = choose;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Choose)) return false;
            Choose o = (Choose) other;
            return choose.equals(o.choose);
        }
        
        @Override
        public int hashCode() {
            return 2 * choose.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.CoalesceStep
     */
    public static final class Coalesce extends TraversalMethod {
        public final CoalesceStep coalesce;
        
        /**
         * Constructs an immutable Coalesce object
         */
        public Coalesce(CoalesceStep coalesce) {
            this.coalesce = coalesce;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Coalesce)) return false;
            Coalesce o = (Coalesce) other;
            return coalesce.equals(o.coalesce);
        }
        
        @Override
        public int hashCode() {
            return 2 * coalesce.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.CoinStep
     */
    public static final class Coin extends TraversalMethod {
        public final CoinStep coin;
        
        /**
         * Constructs an immutable Coin object
         */
        public Coin(CoinStep coin) {
            this.coin = coin;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Coin)) return false;
            Coin o = (Coin) other;
            return coin.equals(o.coin);
        }
        
        @Override
        public int hashCode() {
            return 2 * coin.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.ConnectedComponentStep
     */
    public static final class ConnectedComponent extends TraversalMethod {
        public final ConnectedComponentStep connectedComponent;
        
        /**
         * Constructs an immutable ConnectedComponent object
         */
        public ConnectedComponent(ConnectedComponentStep connectedComponent) {
            this.connectedComponent = connectedComponent;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ConnectedComponent)) return false;
            ConnectedComponent o = (ConnectedComponent) other;
            return connectedComponent.equals(o.connectedComponent);
        }
        
        @Override
        public int hashCode() {
            return 2 * connectedComponent.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.ConstantStep
     */
    public static final class Constant extends TraversalMethod {
        public final ConstantStep constant;
        
        /**
         * Constructs an immutable Constant object
         */
        public Constant(ConstantStep constant) {
            this.constant = constant;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Constant)) return false;
            Constant o = (Constant) other;
            return constant.equals(o.constant);
        }
        
        @Override
        public int hashCode() {
            return 2 * constant.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.CountStep
     */
    public static final class Count extends TraversalMethod {
        public final CountStep count;
        
        /**
         * Constructs an immutable Count object
         */
        public Count(CountStep count) {
            this.count = count;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Count)) return false;
            Count o = (Count) other;
            return count.equals(o.count);
        }
        
        @Override
        public int hashCode() {
            return 2 * count.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.CyclicPathStep
     */
    public static final class CyclicPath extends TraversalMethod {
        public final CyclicPathStep cyclicPath;
        
        /**
         * Constructs an immutable CyclicPath object
         */
        public CyclicPath(CyclicPathStep cyclicPath) {
            this.cyclicPath = cyclicPath;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof CyclicPath)) return false;
            CyclicPath o = (CyclicPath) other;
            return cyclicPath.equals(o.cyclicPath);
        }
        
        @Override
        public int hashCode() {
            return 2 * cyclicPath.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.DedupStep
     */
    public static final class Dedup extends TraversalMethod {
        public final DedupStep dedup;
        
        /**
         * Constructs an immutable Dedup object
         */
        public Dedup(DedupStep dedup) {
            this.dedup = dedup;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Dedup)) return false;
            Dedup o = (Dedup) other;
            return dedup.equals(o.dedup);
        }
        
        @Override
        public int hashCode() {
            return 2 * dedup.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.DropStep
     */
    public static final class Drop extends TraversalMethod {
        public final DropStep drop;
        
        /**
         * Constructs an immutable Drop object
         */
        public Drop(DropStep drop) {
            this.drop = drop;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Drop)) return false;
            Drop o = (Drop) other;
            return drop.equals(o.drop);
        }
        
        @Override
        public int hashCode() {
            return 2 * drop.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.ElementMapStep
     */
    public static final class ElementMap extends TraversalMethod {
        public final ElementMapStep elementMap;
        
        /**
         * Constructs an immutable ElementMap object
         */
        public ElementMap(ElementMapStep elementMap) {
            this.elementMap = elementMap;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ElementMap)) return false;
            ElementMap o = (ElementMap) other;
            return elementMap.equals(o.elementMap);
        }
        
        @Override
        public int hashCode() {
            return 2 * elementMap.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.EmitStep
     */
    public static final class Emit extends TraversalMethod {
        public final EmitStep emit;
        
        /**
         * Constructs an immutable Emit object
         */
        public Emit(EmitStep emit) {
            this.emit = emit;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Emit)) return false;
            Emit o = (Emit) other;
            return emit.equals(o.emit);
        }
        
        @Override
        public int hashCode() {
            return 2 * emit.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.FilterStep
     */
    public static final class Filter extends TraversalMethod {
        public final FilterStep filter;
        
        /**
         * Constructs an immutable Filter object
         */
        public Filter(FilterStep filter) {
            this.filter = filter;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Filter)) return false;
            Filter o = (Filter) other;
            return filter.equals(o.filter);
        }
        
        @Override
        public int hashCode() {
            return 2 * filter.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.FlatMapStep
     */
    public static final class FlatMap extends TraversalMethod {
        public final FlatMapStep flatMap;
        
        /**
         * Constructs an immutable FlatMap object
         */
        public FlatMap(FlatMapStep flatMap) {
            this.flatMap = flatMap;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof FlatMap)) return false;
            FlatMap o = (FlatMap) other;
            return flatMap.equals(o.flatMap);
        }
        
        @Override
        public int hashCode() {
            return 2 * flatMap.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.FoldStep
     */
    public static final class Fold extends TraversalMethod {
        public final FoldStep fold;
        
        /**
         * Constructs an immutable Fold object
         */
        public Fold(FoldStep fold) {
            this.fold = fold;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Fold)) return false;
            Fold o = (Fold) other;
            return fold.equals(o.fold);
        }
        
        @Override
        public int hashCode() {
            return 2 * fold.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.FromStep
     */
    public static final class From extends TraversalMethod {
        public final FromStep from;
        
        /**
         * Constructs an immutable From object
         */
        public From(FromStep from) {
            this.from = from;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof From)) return false;
            From o = (From) other;
            return from.equals(o.from);
        }
        
        @Override
        public int hashCode() {
            return 2 * from.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.GroupStep
     */
    public static final class Group extends TraversalMethod {
        public final GroupStep group;
        
        /**
         * Constructs an immutable Group object
         */
        public Group(GroupStep group) {
            this.group = group;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Group)) return false;
            Group o = (Group) other;
            return group.equals(o.group);
        }
        
        @Override
        public int hashCode() {
            return 2 * group.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.GroupCountStep
     */
    public static final class GroupCount extends TraversalMethod {
        public final GroupCountStep groupCount;
        
        /**
         * Constructs an immutable GroupCount object
         */
        public GroupCount(GroupCountStep groupCount) {
            this.groupCount = groupCount;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof GroupCount)) return false;
            GroupCount o = (GroupCount) other;
            return groupCount.equals(o.groupCount);
        }
        
        @Override
        public int hashCode() {
            return 2 * groupCount.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.HasStep
     */
    public static final class Has extends TraversalMethod {
        public final HasStep has;
        
        /**
         * Constructs an immutable Has object
         */
        public Has(HasStep has) {
            this.has = has;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Has)) return false;
            Has o = (Has) other;
            return has.equals(o.has);
        }
        
        @Override
        public int hashCode() {
            return 2 * has.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.HasIdStep
     */
    public static final class HasId extends TraversalMethod {
        public final HasIdStep hasId;
        
        /**
         * Constructs an immutable HasId object
         */
        public HasId(HasIdStep hasId) {
            this.hasId = hasId;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof HasId)) return false;
            HasId o = (HasId) other;
            return hasId.equals(o.hasId);
        }
        
        @Override
        public int hashCode() {
            return 2 * hasId.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.HasKeyStep
     */
    public static final class HasKey extends TraversalMethod {
        public final HasKeyStep hasKey;
        
        /**
         * Constructs an immutable HasKey object
         */
        public HasKey(HasKeyStep hasKey) {
            this.hasKey = hasKey;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof HasKey)) return false;
            HasKey o = (HasKey) other;
            return hasKey.equals(o.hasKey);
        }
        
        @Override
        public int hashCode() {
            return 2 * hasKey.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.HasLabelStep
     */
    public static final class HasLabel extends TraversalMethod {
        public final HasLabelStep hasLabel;
        
        /**
         * Constructs an immutable HasLabel object
         */
        public HasLabel(HasLabelStep hasLabel) {
            this.hasLabel = hasLabel;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof HasLabel)) return false;
            HasLabel o = (HasLabel) other;
            return hasLabel.equals(o.hasLabel);
        }
        
        @Override
        public int hashCode() {
            return 2 * hasLabel.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.HasNotStep
     */
    public static final class HasNot extends TraversalMethod {
        public final HasNotStep hasNot;
        
        /**
         * Constructs an immutable HasNot object
         */
        public HasNot(HasNotStep hasNot) {
            this.hasNot = hasNot;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof HasNot)) return false;
            HasNot o = (HasNot) other;
            return hasNot.equals(o.hasNot);
        }
        
        @Override
        public int hashCode() {
            return 2 * hasNot.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.HasValueStep
     */
    public static final class HasValue extends TraversalMethod {
        public final HasValueStep hasValue;
        
        /**
         * Constructs an immutable HasValue object
         */
        public HasValue(HasValueStep hasValue) {
            this.hasValue = hasValue;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof HasValue)) return false;
            HasValue o = (HasValue) other;
            return hasValue.equals(o.hasValue);
        }
        
        @Override
        public int hashCode() {
            return 2 * hasValue.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.IdStep
     */
    public static final class Id extends TraversalMethod {
        public final IdStep id;
        
        /**
         * Constructs an immutable Id object
         */
        public Id(IdStep id) {
            this.id = id;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Id)) return false;
            Id o = (Id) other;
            return id.equals(o.id);
        }
        
        @Override
        public int hashCode() {
            return 2 * id.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.IdentityStep
     */
    public static final class Identity extends TraversalMethod {
        public final IdentityStep identity;
        
        /**
         * Constructs an immutable Identity object
         */
        public Identity(IdentityStep identity) {
            this.identity = identity;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Identity)) return false;
            Identity o = (Identity) other;
            return identity.equals(o.identity);
        }
        
        @Override
        public int hashCode() {
            return 2 * identity.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.InStep
     */
    public static final class In extends TraversalMethod {
        public final InStep in;
        
        /**
         * Constructs an immutable In object
         */
        public In(InStep in) {
            this.in = in;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof In)) return false;
            In o = (In) other;
            return in.equals(o.in);
        }
        
        @Override
        public int hashCode() {
            return 2 * in.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.InEStep
     */
    public static final class InE extends TraversalMethod {
        public final InEStep inE;
        
        /**
         * Constructs an immutable InE object
         */
        public InE(InEStep inE) {
            this.inE = inE;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof InE)) return false;
            InE o = (InE) other;
            return inE.equals(o.inE);
        }
        
        @Override
        public int hashCode() {
            return 2 * inE.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.InVStep
     */
    public static final class InV extends TraversalMethod {
        public final InVStep inV;
        
        /**
         * Constructs an immutable InV object
         */
        public InV(InVStep inV) {
            this.inV = inV;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof InV)) return false;
            InV o = (InV) other;
            return inV.equals(o.inV);
        }
        
        @Override
        public int hashCode() {
            return 2 * inV.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.IndexStep
     */
    public static final class Index extends TraversalMethod {
        public final IndexStep index;
        
        /**
         * Constructs an immutable Index object
         */
        public Index(IndexStep index) {
            this.index = index;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Index)) return false;
            Index o = (Index) other;
            return index.equals(o.index);
        }
        
        @Override
        public int hashCode() {
            return 2 * index.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.InjectStep
     */
    public static final class Inject extends TraversalMethod {
        public final InjectStep inject;
        
        /**
         * Constructs an immutable Inject object
         */
        public Inject(InjectStep inject) {
            this.inject = inject;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Inject)) return false;
            Inject o = (Inject) other;
            return inject.equals(o.inject);
        }
        
        @Override
        public int hashCode() {
            return 2 * inject.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.IsStep
     */
    public static final class Is extends TraversalMethod {
        public final IsStep is;
        
        /**
         * Constructs an immutable Is object
         */
        public Is(IsStep is) {
            this.is = is;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Is)) return false;
            Is o = (Is) other;
            return is.equals(o.is);
        }
        
        @Override
        public int hashCode() {
            return 2 * is.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.KeyStep
     */
    public static final class Key extends TraversalMethod {
        public final KeyStep key;
        
        /**
         * Constructs an immutable Key object
         */
        public Key(KeyStep key) {
            this.key = key;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Key)) return false;
            Key o = (Key) other;
            return key.equals(o.key);
        }
        
        @Override
        public int hashCode() {
            return 2 * key.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.LabelStep
     */
    public static final class Label extends TraversalMethod {
        public final LabelStep label;
        
        /**
         * Constructs an immutable Label object
         */
        public Label(LabelStep label) {
            this.label = label;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Label)) return false;
            Label o = (Label) other;
            return label.equals(o.label);
        }
        
        @Override
        public int hashCode() {
            return 2 * label.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.LimitStep
     */
    public static final class Limit extends TraversalMethod {
        public final LimitStep limit;
        
        /**
         * Constructs an immutable Limit object
         */
        public Limit(LimitStep limit) {
            this.limit = limit;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Limit)) return false;
            Limit o = (Limit) other;
            return limit.equals(o.limit);
        }
        
        @Override
        public int hashCode() {
            return 2 * limit.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.LocalStep
     */
    public static final class Local extends TraversalMethod {
        public final LocalStep local;
        
        /**
         * Constructs an immutable Local object
         */
        public Local(LocalStep local) {
            this.local = local;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Local)) return false;
            Local o = (Local) other;
            return local.equals(o.local);
        }
        
        @Override
        public int hashCode() {
            return 2 * local.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.LoopsStep
     */
    public static final class Loops extends TraversalMethod {
        public final LoopsStep loops;
        
        /**
         * Constructs an immutable Loops object
         */
        public Loops(LoopsStep loops) {
            this.loops = loops;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Loops)) return false;
            Loops o = (Loops) other;
            return loops.equals(o.loops);
        }
        
        @Override
        public int hashCode() {
            return 2 * loops.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.MapStep
     */
    public static final class Map extends TraversalMethod {
        public final MapStep map;
        
        /**
         * Constructs an immutable Map object
         */
        public Map(MapStep map) {
            this.map = map;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Map)) return false;
            Map o = (Map) other;
            return map.equals(o.map);
        }
        
        @Override
        public int hashCode() {
            return 2 * map.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.MatchStep
     */
    public static final class Match extends TraversalMethod {
        public final MatchStep match;
        
        /**
         * Constructs an immutable Match object
         */
        public Match(MatchStep match) {
            this.match = match;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Match)) return false;
            Match o = (Match) other;
            return match.equals(o.match);
        }
        
        @Override
        public int hashCode() {
            return 2 * match.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.MathStep
     */
    public static final class MathEsc extends TraversalMethod {
        public final MathStep math;
        
        /**
         * Constructs an immutable MathEsc object
         */
        public MathEsc(MathStep math) {
            this.math = math;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof MathEsc)) return false;
            MathEsc o = (MathEsc) other;
            return math.equals(o.math);
        }
        
        @Override
        public int hashCode() {
            return 2 * math.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.MaxStep
     */
    public static final class Max extends TraversalMethod {
        public final MaxStep max;
        
        /**
         * Constructs an immutable Max object
         */
        public Max(MaxStep max) {
            this.max = max;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Max)) return false;
            Max o = (Max) other;
            return max.equals(o.max);
        }
        
        @Override
        public int hashCode() {
            return 2 * max.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.MeanStep
     */
    public static final class Mean extends TraversalMethod {
        public final MeanStep mean;
        
        /**
         * Constructs an immutable Mean object
         */
        public Mean(MeanStep mean) {
            this.mean = mean;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Mean)) return false;
            Mean o = (Mean) other;
            return mean.equals(o.mean);
        }
        
        @Override
        public int hashCode() {
            return 2 * mean.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.MinStep
     */
    public static final class Min extends TraversalMethod {
        public final MinStep min;
        
        /**
         * Constructs an immutable Min object
         */
        public Min(MinStep min) {
            this.min = min;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Min)) return false;
            Min o = (Min) other;
            return min.equals(o.min);
        }
        
        @Override
        public int hashCode() {
            return 2 * min.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.NotStep
     */
    public static final class Not extends TraversalMethod {
        public final NotStep not;
        
        /**
         * Constructs an immutable Not object
         */
        public Not(NotStep not) {
            this.not = not;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Not)) return false;
            Not o = (Not) other;
            return not.equals(o.not);
        }
        
        @Override
        public int hashCode() {
            return 2 * not.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.OptionStep
     */
    public static final class Option extends TraversalMethod {
        public final OptionStep option;
        
        /**
         * Constructs an immutable Option object
         */
        public Option(OptionStep option) {
            this.option = option;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Option)) return false;
            Option o = (Option) other;
            return option.equals(o.option);
        }
        
        @Override
        public int hashCode() {
            return 2 * option.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.OptionalStep
     */
    public static final class Optional extends TraversalMethod {
        public final OptionalStep optional;
        
        /**
         * Constructs an immutable Optional object
         */
        public Optional(OptionalStep optional) {
            this.optional = optional;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Optional)) return false;
            Optional o = (Optional) other;
            return optional.equals(o.optional);
        }
        
        @Override
        public int hashCode() {
            return 2 * optional.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.OrStep
     */
    public static final class Or extends TraversalMethod {
        public final OrStep or;
        
        /**
         * Constructs an immutable Or object
         */
        public Or(OrStep or) {
            this.or = or;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Or)) return false;
            Or o = (Or) other;
            return or.equals(o.or);
        }
        
        @Override
        public int hashCode() {
            return 2 * or.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.OrderStep
     */
    public static final class Order extends TraversalMethod {
        public final OrderStep order;
        
        /**
         * Constructs an immutable Order object
         */
        public Order(OrderStep order) {
            this.order = order;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Order)) return false;
            Order o = (Order) other;
            return order.equals(o.order);
        }
        
        @Override
        public int hashCode() {
            return 2 * order.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.OtherVStep
     */
    public static final class OtherV extends TraversalMethod {
        public final OtherVStep otherV;
        
        /**
         * Constructs an immutable OtherV object
         */
        public OtherV(OtherVStep otherV) {
            this.otherV = otherV;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof OtherV)) return false;
            OtherV o = (OtherV) other;
            return otherV.equals(o.otherV);
        }
        
        @Override
        public int hashCode() {
            return 2 * otherV.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.OutStep
     */
    public static final class Out extends TraversalMethod {
        public final OutStep out;
        
        /**
         * Constructs an immutable Out object
         */
        public Out(OutStep out) {
            this.out = out;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Out)) return false;
            Out o = (Out) other;
            return out.equals(o.out);
        }
        
        @Override
        public int hashCode() {
            return 2 * out.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.OutEStep
     */
    public static final class OutE extends TraversalMethod {
        public final OutEStep outE;
        
        /**
         * Constructs an immutable OutE object
         */
        public OutE(OutEStep outE) {
            this.outE = outE;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof OutE)) return false;
            OutE o = (OutE) other;
            return outE.equals(o.outE);
        }
        
        @Override
        public int hashCode() {
            return 2 * outE.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.OutVStep
     */
    public static final class OutV extends TraversalMethod {
        public final OutVStep outV;
        
        /**
         * Constructs an immutable OutV object
         */
        public OutV(OutVStep outV) {
            this.outV = outV;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof OutV)) return false;
            OutV o = (OutV) other;
            return outV.equals(o.outV);
        }
        
        @Override
        public int hashCode() {
            return 2 * outV.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.PageRankStep
     */
    public static final class PageRank extends TraversalMethod {
        public final PageRankStep pageRank;
        
        /**
         * Constructs an immutable PageRank object
         */
        public PageRank(PageRankStep pageRank) {
            this.pageRank = pageRank;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof PageRank)) return false;
            PageRank o = (PageRank) other;
            return pageRank.equals(o.pageRank);
        }
        
        @Override
        public int hashCode() {
            return 2 * pageRank.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.PathStep
     */
    public static final class Path extends TraversalMethod {
        public final PathStep path;
        
        /**
         * Constructs an immutable Path object
         */
        public Path(PathStep path) {
            this.path = path;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Path)) return false;
            Path o = (Path) other;
            return path.equals(o.path);
        }
        
        @Override
        public int hashCode() {
            return 2 * path.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.PeerPressureStep
     */
    public static final class PeerPressure extends TraversalMethod {
        public final PeerPressureStep peerPressure;
        
        /**
         * Constructs an immutable PeerPressure object
         */
        public PeerPressure(PeerPressureStep peerPressure) {
            this.peerPressure = peerPressure;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof PeerPressure)) return false;
            PeerPressure o = (PeerPressure) other;
            return peerPressure.equals(o.peerPressure);
        }
        
        @Override
        public int hashCode() {
            return 2 * peerPressure.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.ProfileStep
     */
    public static final class Profile extends TraversalMethod {
        public final ProfileStep profile;
        
        /**
         * Constructs an immutable Profile object
         */
        public Profile(ProfileStep profile) {
            this.profile = profile;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Profile)) return false;
            Profile o = (Profile) other;
            return profile.equals(o.profile);
        }
        
        @Override
        public int hashCode() {
            return 2 * profile.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.ProjectStep
     */
    public static final class Project extends TraversalMethod {
        public final ProjectStep project;
        
        /**
         * Constructs an immutable Project object
         */
        public Project(ProjectStep project) {
            this.project = project;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Project)) return false;
            Project o = (Project) other;
            return project.equals(o.project);
        }
        
        @Override
        public int hashCode() {
            return 2 * project.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.PropertiesStep
     */
    public static final class Properties extends TraversalMethod {
        public final PropertiesStep properties;
        
        /**
         * Constructs an immutable Properties object
         */
        public Properties(PropertiesStep properties) {
            this.properties = properties;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Properties)) return false;
            Properties o = (Properties) other;
            return properties.equals(o.properties);
        }
        
        @Override
        public int hashCode() {
            return 2 * properties.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.PropertyStep
     */
    public static final class Property extends TraversalMethod {
        public final PropertyStep property;
        
        /**
         * Constructs an immutable Property object
         */
        public Property(PropertyStep property) {
            this.property = property;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Property)) return false;
            Property o = (Property) other;
            return property.equals(o.property);
        }
        
        @Override
        public int hashCode() {
            return 2 * property.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.PropertyMapStep
     */
    public static final class PropertyMap extends TraversalMethod {
        public final PropertyMapStep propertyMap;
        
        /**
         * Constructs an immutable PropertyMap object
         */
        public PropertyMap(PropertyMapStep propertyMap) {
            this.propertyMap = propertyMap;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof PropertyMap)) return false;
            PropertyMap o = (PropertyMap) other;
            return propertyMap.equals(o.propertyMap);
        }
        
        @Override
        public int hashCode() {
            return 2 * propertyMap.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.RangeStep
     */
    public static final class Range extends TraversalMethod {
        public final RangeStep range;
        
        /**
         * Constructs an immutable Range object
         */
        public Range(RangeStep range) {
            this.range = range;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Range)) return false;
            Range o = (Range) other;
            return range.equals(o.range);
        }
        
        @Override
        public int hashCode() {
            return 2 * range.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.ReadStep
     */
    public static final class Read extends TraversalMethod {
        public final ReadStep read;
        
        /**
         * Constructs an immutable Read object
         */
        public Read(ReadStep read) {
            this.read = read;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Read)) return false;
            Read o = (Read) other;
            return read.equals(o.read);
        }
        
        @Override
        public int hashCode() {
            return 2 * read.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.RepeatStep
     */
    public static final class Repeat extends TraversalMethod {
        public final RepeatStep repeat;
        
        /**
         * Constructs an immutable Repeat object
         */
        public Repeat(RepeatStep repeat) {
            this.repeat = repeat;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Repeat)) return false;
            Repeat o = (Repeat) other;
            return repeat.equals(o.repeat);
        }
        
        @Override
        public int hashCode() {
            return 2 * repeat.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.SackStep
     */
    public static final class Sack extends TraversalMethod {
        public final SackStep sack;
        
        /**
         * Constructs an immutable Sack object
         */
        public Sack(SackStep sack) {
            this.sack = sack;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Sack)) return false;
            Sack o = (Sack) other;
            return sack.equals(o.sack);
        }
        
        @Override
        public int hashCode() {
            return 2 * sack.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.SampleStep
     */
    public static final class Sample extends TraversalMethod {
        public final SampleStep sample;
        
        /**
         * Constructs an immutable Sample object
         */
        public Sample(SampleStep sample) {
            this.sample = sample;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Sample)) return false;
            Sample o = (Sample) other;
            return sample.equals(o.sample);
        }
        
        @Override
        public int hashCode() {
            return 2 * sample.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.SelectStep
     */
    public static final class Select extends TraversalMethod {
        public final SelectStep select;
        
        /**
         * Constructs an immutable Select object
         */
        public Select(SelectStep select) {
            this.select = select;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Select)) return false;
            Select o = (Select) other;
            return select.equals(o.select);
        }
        
        @Override
        public int hashCode() {
            return 2 * select.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.ShortestPathStep
     */
    public static final class ShortestPath extends TraversalMethod {
        public final ShortestPathStep shortestPath;
        
        /**
         * Constructs an immutable ShortestPath object
         */
        public ShortestPath(ShortestPathStep shortestPath) {
            this.shortestPath = shortestPath;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ShortestPath)) return false;
            ShortestPath o = (ShortestPath) other;
            return shortestPath.equals(o.shortestPath);
        }
        
        @Override
        public int hashCode() {
            return 2 * shortestPath.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.SideEffectStep
     */
    public static final class SideEffect extends TraversalMethod {
        public final SideEffectStep sideEffect;
        
        /**
         * Constructs an immutable SideEffect object
         */
        public SideEffect(SideEffectStep sideEffect) {
            this.sideEffect = sideEffect;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof SideEffect)) return false;
            SideEffect o = (SideEffect) other;
            return sideEffect.equals(o.sideEffect);
        }
        
        @Override
        public int hashCode() {
            return 2 * sideEffect.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.SimplePathStep
     */
    public static final class SimplePath extends TraversalMethod {
        public final SimplePathStep simplePath;
        
        /**
         * Constructs an immutable SimplePath object
         */
        public SimplePath(SimplePathStep simplePath) {
            this.simplePath = simplePath;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof SimplePath)) return false;
            SimplePath o = (SimplePath) other;
            return simplePath.equals(o.simplePath);
        }
        
        @Override
        public int hashCode() {
            return 2 * simplePath.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.SkipStep
     */
    public static final class Skip extends TraversalMethod {
        public final SkipStep skip;
        
        /**
         * Constructs an immutable Skip object
         */
        public Skip(SkipStep skip) {
            this.skip = skip;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Skip)) return false;
            Skip o = (Skip) other;
            return skip.equals(o.skip);
        }
        
        @Override
        public int hashCode() {
            return 2 * skip.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.StoreStep
     */
    public static final class Store extends TraversalMethod {
        public final StoreStep store;
        
        /**
         * Constructs an immutable Store object
         */
        public Store(StoreStep store) {
            this.store = store;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Store)) return false;
            Store o = (Store) other;
            return store.equals(o.store);
        }
        
        @Override
        public int hashCode() {
            return 2 * store.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.SubgraphStep
     */
    public static final class Subgraph extends TraversalMethod {
        public final SubgraphStep subgraph;
        
        /**
         * Constructs an immutable Subgraph object
         */
        public Subgraph(SubgraphStep subgraph) {
            this.subgraph = subgraph;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Subgraph)) return false;
            Subgraph o = (Subgraph) other;
            return subgraph.equals(o.subgraph);
        }
        
        @Override
        public int hashCode() {
            return 2 * subgraph.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.SumStep
     */
    public static final class Sum extends TraversalMethod {
        public final SumStep sum;
        
        /**
         * Constructs an immutable Sum object
         */
        public Sum(SumStep sum) {
            this.sum = sum;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Sum)) return false;
            Sum o = (Sum) other;
            return sum.equals(o.sum);
        }
        
        @Override
        public int hashCode() {
            return 2 * sum.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.TailStep
     */
    public static final class Tail extends TraversalMethod {
        public final TailStep tail;
        
        /**
         * Constructs an immutable Tail object
         */
        public Tail(TailStep tail) {
            this.tail = tail;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Tail)) return false;
            Tail o = (Tail) other;
            return tail.equals(o.tail);
        }
        
        @Override
        public int hashCode() {
            return 2 * tail.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.TimeLimitStep
     */
    public static final class TimeLimit extends TraversalMethod {
        public final TimeLimitStep timeLimit;
        
        /**
         * Constructs an immutable TimeLimit object
         */
        public TimeLimit(TimeLimitStep timeLimit) {
            this.timeLimit = timeLimit;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof TimeLimit)) return false;
            TimeLimit o = (TimeLimit) other;
            return timeLimit.equals(o.timeLimit);
        }
        
        @Override
        public int hashCode() {
            return 2 * timeLimit.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.TimesStep
     */
    public static final class Times extends TraversalMethod {
        public final TimesStep times;
        
        /**
         * Constructs an immutable Times object
         */
        public Times(TimesStep times) {
            this.times = times;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Times)) return false;
            Times o = (Times) other;
            return times.equals(o.times);
        }
        
        @Override
        public int hashCode() {
            return 2 * times.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.ToStep
     */
    public static final class To extends TraversalMethod {
        public final ToStep to;
        
        /**
         * Constructs an immutable To object
         */
        public To(ToStep to) {
            this.to = to;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof To)) return false;
            To o = (To) other;
            return to.equals(o.to);
        }
        
        @Override
        public int hashCode() {
            return 2 * to.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.ToEStep
     */
    public static final class ToE extends TraversalMethod {
        public final ToEStep toE;
        
        /**
         * Constructs an immutable ToE object
         */
        public ToE(ToEStep toE) {
            this.toE = toE;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ToE)) return false;
            ToE o = (ToE) other;
            return toE.equals(o.toE);
        }
        
        @Override
        public int hashCode() {
            return 2 * toE.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.ToVStep
     */
    public static final class ToV extends TraversalMethod {
        public final ToVStep toV;
        
        /**
         * Constructs an immutable ToV object
         */
        public ToV(ToVStep toV) {
            this.toV = toV;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ToV)) return false;
            ToV o = (ToV) other;
            return toV.equals(o.toV);
        }
        
        @Override
        public int hashCode() {
            return 2 * toV.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.TreeStep
     */
    public static final class Tree extends TraversalMethod {
        public final TreeStep tree;
        
        /**
         * Constructs an immutable Tree object
         */
        public Tree(TreeStep tree) {
            this.tree = tree;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Tree)) return false;
            Tree o = (Tree) other;
            return tree.equals(o.tree);
        }
        
        @Override
        public int hashCode() {
            return 2 * tree.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.UnfoldStep
     */
    public static final class Unfold extends TraversalMethod {
        public final UnfoldStep unfold;
        
        /**
         * Constructs an immutable Unfold object
         */
        public Unfold(UnfoldStep unfold) {
            this.unfold = unfold;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Unfold)) return false;
            Unfold o = (Unfold) other;
            return unfold.equals(o.unfold);
        }
        
        @Override
        public int hashCode() {
            return 2 * unfold.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.UnionStep
     */
    public static final class Union extends TraversalMethod {
        public final UnionStep union;
        
        /**
         * Constructs an immutable Union object
         */
        public Union(UnionStep union) {
            this.union = union;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Union)) return false;
            Union o = (Union) other;
            return union.equals(o.union);
        }
        
        @Override
        public int hashCode() {
            return 2 * union.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.UntilStep
     */
    public static final class Until extends TraversalMethod {
        public final UntilStep until;
        
        /**
         * Constructs an immutable Until object
         */
        public Until(UntilStep until) {
            this.until = until;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Until)) return false;
            Until o = (Until) other;
            return until.equals(o.until);
        }
        
        @Override
        public int hashCode() {
            return 2 * until.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.ValueStep
     */
    public static final class Value extends TraversalMethod {
        public final ValueStep value;
        
        /**
         * Constructs an immutable Value object
         */
        public Value(ValueStep value) {
            this.value = value;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Value)) return false;
            Value o = (Value) other;
            return value.equals(o.value);
        }
        
        @Override
        public int hashCode() {
            return 2 * value.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.ValueMapStep
     */
    public static final class ValueMap extends TraversalMethod {
        public final ValueMapStep valueMap;
        
        /**
         * Constructs an immutable ValueMap object
         */
        public ValueMap(ValueMapStep valueMap) {
            this.valueMap = valueMap;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ValueMap)) return false;
            ValueMap o = (ValueMap) other;
            return valueMap.equals(o.valueMap);
        }
        
        @Override
        public int hashCode() {
            return 2 * valueMap.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.ValuesStep
     */
    public static final class Values extends TraversalMethod {
        public final ValuesStep values;
        
        /**
         * Constructs an immutable Values object
         */
        public Values(ValuesStep values) {
            this.values = values;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Values)) return false;
            Values o = (Values) other;
            return values.equals(o.values);
        }
        
        @Override
        public int hashCode() {
            return 2 * values.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.WhereStep
     */
    public static final class Where extends TraversalMethod {
        public final WhereStep where;
        
        /**
         * Constructs an immutable Where object
         */
        public Where(WhereStep where) {
            this.where = where;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Where)) return false;
            Where o = (Where) other;
            return where.equals(o.where);
        }
        
        @Override
        public int hashCode() {
            return 2 * where.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.WithStep
     */
    public static final class With extends TraversalMethod {
        public final WithStep with;
        
        /**
         * Constructs an immutable With object
         */
        public With(WithStep with) {
            this.with = with;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof With)) return false;
            With o = (With) other;
            return with.equals(o.with);
        }
        
        @Override
        public int hashCode() {
            return 2 * with.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.WriteStep
     */
    public static final class Write extends TraversalMethod {
        public final WriteStep write;
        
        /**
         * Constructs an immutable Write object
         */
        public Write(WriteStep write) {
            this.write = write;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Write)) return false;
            Write o = (Write) other;
            return write.equals(o.write);
        }
        
        @Override
        public int hashCode() {
            return 2 * write.hashCode();
        }
    }
}
