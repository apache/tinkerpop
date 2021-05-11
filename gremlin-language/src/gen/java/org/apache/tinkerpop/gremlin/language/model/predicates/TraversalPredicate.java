package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

public abstract class TraversalPredicate {
    private TraversalPredicate() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    public static class NegateValue {
        /**
         * Constructs an immutable NegateValue object
         */
        public NegateValue() {}
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof NegateValue)) return false;
            NegateValue o = (NegateValue) other;
            return true;
        }
        
        @Override
        public int hashCode() {
            return 0;
        }
    }
    
    /**
     * An interface for applying a function to a TraversalPredicate according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Eq instance) ;
        
        R visit(Neq instance) ;
        
        R visit(Lt instance) ;
        
        R visit(Lte instance) ;
        
        R visit(Gt instance) ;
        
        R visit(Gte instance) ;
        
        R visit(Inside instance) ;
        
        R visit(Outside instance) ;
        
        R visit(Between instance) ;
        
        R visit(Within instance) ;
        
        R visit(Without instance) ;
        
        R visit(Not instance) ;
        
        R visit(StartingWith instance) ;
        
        R visit(NotStartingWith instance) ;
        
        R visit(EndingWith instance) ;
        
        R visit(Containing instance) ;
        
        R visit(And instance) ;
        
        R visit(Or instance) ;
        
        R visit(Negate instance) ;
    }
    
    /**
     * An interface for applying a function to a TraversalPredicate according to its variant (subclass). If a visit() method
     * for a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(TraversalPredicate instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(Eq instance) {
            return otherwise(instance);
        }
        
        default R visit(Neq instance) {
            return otherwise(instance);
        }
        
        default R visit(Lt instance) {
            return otherwise(instance);
        }
        
        default R visit(Lte instance) {
            return otherwise(instance);
        }
        
        default R visit(Gt instance) {
            return otherwise(instance);
        }
        
        default R visit(Gte instance) {
            return otherwise(instance);
        }
        
        default R visit(Inside instance) {
            return otherwise(instance);
        }
        
        default R visit(Outside instance) {
            return otherwise(instance);
        }
        
        default R visit(Between instance) {
            return otherwise(instance);
        }
        
        default R visit(Within instance) {
            return otherwise(instance);
        }
        
        default R visit(Without instance) {
            return otherwise(instance);
        }
        
        default R visit(Not instance) {
            return otherwise(instance);
        }
        
        default R visit(StartingWith instance) {
            return otherwise(instance);
        }
        
        default R visit(NotStartingWith instance) {
            return otherwise(instance);
        }
        
        default R visit(EndingWith instance) {
            return otherwise(instance);
        }
        
        default R visit(Containing instance) {
            return otherwise(instance);
        }
        
        default R visit(And instance) {
            return otherwise(instance);
        }
        
        default R visit(Or instance) {
            return otherwise(instance);
        }
        
        default R visit(Negate instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.Eq
     */
    public static final class Eq extends TraversalPredicate {
        public final Eq eq;
        
        /**
         * Constructs an immutable Eq object
         */
        public Eq(Eq eq) {
            this.eq = eq;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Eq)) return false;
            Eq o = (Eq) other;
            return eq.equals(o.eq);
        }
        
        @Override
        public int hashCode() {
            return 2 * eq.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.Neq
     */
    public static final class Neq extends TraversalPredicate {
        public final Neq neq;
        
        /**
         * Constructs an immutable Neq object
         */
        public Neq(Neq neq) {
            this.neq = neq;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Neq)) return false;
            Neq o = (Neq) other;
            return neq.equals(o.neq);
        }
        
        @Override
        public int hashCode() {
            return 2 * neq.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.Lt
     */
    public static final class Lt extends TraversalPredicate {
        public final Lt lt;
        
        /**
         * Constructs an immutable Lt object
         */
        public Lt(Lt lt) {
            this.lt = lt;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Lt)) return false;
            Lt o = (Lt) other;
            return lt.equals(o.lt);
        }
        
        @Override
        public int hashCode() {
            return 2 * lt.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.Lte
     */
    public static final class Lte extends TraversalPredicate {
        public final Lte lte;
        
        /**
         * Constructs an immutable Lte object
         */
        public Lte(Lte lte) {
            this.lte = lte;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Lte)) return false;
            Lte o = (Lte) other;
            return lte.equals(o.lte);
        }
        
        @Override
        public int hashCode() {
            return 2 * lte.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.Gt
     */
    public static final class Gt extends TraversalPredicate {
        public final Gt gt;
        
        /**
         * Constructs an immutable Gt object
         */
        public Gt(Gt gt) {
            this.gt = gt;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Gt)) return false;
            Gt o = (Gt) other;
            return gt.equals(o.gt);
        }
        
        @Override
        public int hashCode() {
            return 2 * gt.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.Gte
     */
    public static final class Gte extends TraversalPredicate {
        public final Gte gte;
        
        /**
         * Constructs an immutable Gte object
         */
        public Gte(Gte gte) {
            this.gte = gte;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Gte)) return false;
            Gte o = (Gte) other;
            return gte.equals(o.gte);
        }
        
        @Override
        public int hashCode() {
            return 2 * gte.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.Inside
     */
    public static final class Inside extends TraversalPredicate {
        public final Inside inside;
        
        /**
         * Constructs an immutable Inside object
         */
        public Inside(Inside inside) {
            this.inside = inside;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Inside)) return false;
            Inside o = (Inside) other;
            return inside.equals(o.inside);
        }
        
        @Override
        public int hashCode() {
            return 2 * inside.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.Outside
     */
    public static final class Outside extends TraversalPredicate {
        public final Outside outside;
        
        /**
         * Constructs an immutable Outside object
         */
        public Outside(Outside outside) {
            this.outside = outside;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Outside)) return false;
            Outside o = (Outside) other;
            return outside.equals(o.outside);
        }
        
        @Override
        public int hashCode() {
            return 2 * outside.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.Between
     */
    public static final class Between extends TraversalPredicate {
        public final Between between;
        
        /**
         * Constructs an immutable Between object
         */
        public Between(Between between) {
            this.between = between;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Between)) return false;
            Between o = (Between) other;
            return between.equals(o.between);
        }
        
        @Override
        public int hashCode() {
            return 2 * between.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.Within
     */
    public static final class Within extends TraversalPredicate {
        public final Within within;
        
        /**
         * Constructs an immutable Within object
         */
        public Within(Within within) {
            this.within = within;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Within)) return false;
            Within o = (Within) other;
            return within.equals(o.within);
        }
        
        @Override
        public int hashCode() {
            return 2 * within.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.Without
     */
    public static final class Without extends TraversalPredicate {
        public final Without without;
        
        /**
         * Constructs an immutable Without object
         */
        public Without(Without without) {
            this.without = without;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Without)) return false;
            Without o = (Without) other;
            return without.equals(o.without);
        }
        
        @Override
        public int hashCode() {
            return 2 * without.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.Not
     */
    public static final class Not extends TraversalPredicate {
        public final Not not;
        
        /**
         * Constructs an immutable Not object
         */
        public Not(Not not) {
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
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.StartingWith
     */
    public static final class StartingWith extends TraversalPredicate {
        public final StartingWith startingWith;
        
        /**
         * Constructs an immutable StartingWith object
         */
        public StartingWith(StartingWith startingWith) {
            this.startingWith = startingWith;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof StartingWith)) return false;
            StartingWith o = (StartingWith) other;
            return startingWith.equals(o.startingWith);
        }
        
        @Override
        public int hashCode() {
            return 2 * startingWith.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.NotStartingWith
     */
    public static final class NotStartingWith extends TraversalPredicate {
        public final NotStartingWith notStartingWith;
        
        /**
         * Constructs an immutable NotStartingWith object
         */
        public NotStartingWith(NotStartingWith notStartingWith) {
            this.notStartingWith = notStartingWith;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof NotStartingWith)) return false;
            NotStartingWith o = (NotStartingWith) other;
            return notStartingWith.equals(o.notStartingWith);
        }
        
        @Override
        public int hashCode() {
            return 2 * notStartingWith.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.NotEndingWith
     */
    public static final class EndingWith extends TraversalPredicate {
        public final NotEndingWith endingWith;
        
        /**
         * Constructs an immutable EndingWith object
         */
        public EndingWith(NotEndingWith endingWith) {
            this.endingWith = endingWith;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof EndingWith)) return false;
            EndingWith o = (EndingWith) other;
            return endingWith.equals(o.endingWith);
        }
        
        @Override
        public int hashCode() {
            return 2 * endingWith.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.NotContaining
     */
    public static final class Containing extends TraversalPredicate {
        public final NotContaining containing;
        
        /**
         * Constructs an immutable Containing object
         */
        public Containing(NotContaining containing) {
            this.containing = containing;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Containing)) return false;
            Containing o = (Containing) other;
            return containing.equals(o.containing);
        }
        
        @Override
        public int hashCode() {
            return 2 * containing.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
     */
    public static final class And extends TraversalPredicate {
        public final TraversalPredicate and;
        
        /**
         * Constructs an immutable And object
         */
        public And(TraversalPredicate and) {
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
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
     */
    public static final class Or extends TraversalPredicate {
        public final TraversalPredicate or;
        
        /**
         * Constructs an immutable Or object
         */
        public Or(TraversalPredicate or) {
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
    
    public static final class Negate extends TraversalPredicate {
        public final NegateValue negate;
        
        /**
         * Constructs an immutable Negate object
         */
        public Negate(NegateValue negate) {
            this.negate = negate;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Negate)) return false;
            Negate o = (Negate) other;
            return negate.equals(o.negate);
        }
        
        @Override
        public int hashCode() {
            return 2 * negate.hashCode();
        }
    }
}
