package com.tinkerpop.gremlin.process.graph.step.map.match;

import java.util.function.BiPredicate;

/**
 * An Enumerator which finds the Cartesian product of two other Enumerators, expanding at the same rate in either dimension.
 * This maximizes the size of the product with respect to the number of expansions of the base Enumerators.
 */
public class CartesianEnumerator<T> implements Enumerator<T> {
    private final Enumerator<T> xEnum, yEnum;

    public CartesianEnumerator(final Enumerator<T> xEnum,
                               final Enumerator<T> yEnum) {
        this.xEnum = xEnum;
        this.yEnum = yEnum;
    }

    public int size() {
        return xEnum.size() * yEnum.size();
    }

    public boolean isComplete() {
        boolean xc = xEnum.isComplete(), yc = yEnum.isComplete();
        return xc && 0 == xEnum.size()
                || yc && 0 == yEnum.size()
                || xc && yc;
    }

    public boolean visitSolution(final int i,
                                 final BiPredicate<String, T> visitor) {
        int sq = (int) Math.sqrt(i);

        // choose x and y such that the solution represented by i
        // remains constant as this Enumerator expands
        int x, y;

        if (0 == i) {
            x = y = 0;
        } else {
            int r = i - sq * sq;
            if (r < sq) {
                x = sq;
                y = r;
            } else {
                x = r - sq;
                y = sq;
            }
        }

        // expand x
        while (sq >= xEnum.size()) {
            // ran into x limit
            if (xEnum.isComplete()) {
                if (0 == xEnum.size()) {
                    return false;
                }
                x = i % xEnum.size();
                y = i / xEnum.size();
                break;
            }
            if (!xEnum.visitSolution(xEnum.size(), (BiPredicate<String, T>) MatchStepNew.TRIVIAL_VISITOR)) return false;
        }

        int height = i / Math.min(1 + sq, xEnum.size());

        // expand y
        while (height >= yEnum.size()) {
            // ran into y limit; expand x again
            if (yEnum.isComplete()) {
                if (0 == yEnum.size()) {
                    return false;
                }
                height = yEnum.size();
                int width = i / height;
                while (width >= xEnum.size()) {
                    if (!xEnum.visitSolution(xEnum.size(), (BiPredicate<String, T>) MatchStepNew.TRIVIAL_VISITOR)) return false;
                }
                x = i / yEnum.size();
                y = i % yEnum.size();
                break;
            }

            if (!yEnum.visitSolution(yEnum.size(), (BiPredicate<String, T>) MatchStepNew.TRIVIAL_VISITOR)) return false;
        }

        // solutions are visited completely (if we have reached this point), else not at all
        return xEnum.visitSolution(x, visitor) && yEnum.visitSolution(y, visitor);
    }
}
