package com.tinkerpop.gremlin.process.graph.step.map.match;

import java.util.function.BiConsumer;

/**
 * An Enumerator which finds the Cartesian product of two other Enumerators, expanding at the same rate in either dimension.
 * This maximizes the size of the product with respect to the number of expansions of the base Enumerators.
 */
public class CrossJoinEnumerator<T> implements Enumerator<T> {
    private final Enumerator<T> xEnum, yEnum;

    public CrossJoinEnumerator(final Enumerator<T> xEnum,
                               final Enumerator<T> yEnum) {
        this.xEnum = xEnum;
        this.yEnum = yEnum;
    }

    @Override
    public int size() {
        return xEnum.size() * yEnum.size();
    }

    @Override
    public boolean isComplete() {
        boolean xc = xEnum.isComplete(), yc = yEnum.isComplete();
        return xc && 0 == xEnum.size()
                || yc && 0 == yEnum.size()
                || xc && yc;
    }

    // note: permits random access
    @Override
    public boolean visitSolution(final int index,
                                 final BiConsumer<String, T> visitor) {
        int sq = (int) Math.sqrt(index);

        // choose x and y such that the solution represented by i
        // remains constant as this Enumerator expands
        int x, y;

        if (0 == index) {
            x = y = 0;
        } else {
            int r = index - sq * sq;
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
                x = index % xEnum.size();
                y = index / xEnum.size();
                break;
            }
            if (!xEnum.visitSolution(xEnum.size(), (BiConsumer<String, T>) MatchStep.TRIVIAL_CONSUMER)) return false;
        }

        int height = index / Math.min(1 + sq, xEnum.size());

        // expand y
        while (height >= yEnum.size()) {
            // ran into y limit; expand x again
            if (yEnum.isComplete()) {
                if (0 == yEnum.size()) {
                    return false;
                }
                height = yEnum.size();
                int width = index / height;
                while (width >= xEnum.size()) {
                    if (!xEnum.visitSolution(xEnum.size(), (BiConsumer<String, T>) MatchStep.TRIVIAL_CONSUMER)) return false;
                }
                x = index / yEnum.size();
                y = index % yEnum.size();
                break;
            }

            if (!yEnum.visitSolution(yEnum.size(), (BiConsumer<String, T>) MatchStep.TRIVIAL_CONSUMER)) return false;
        }

        // solutions are visited completely (if we have reached this point), else not at all
        return xEnum.visitSolution(x, visitor) && yEnum.visitSolution(y, visitor);
    }
}
