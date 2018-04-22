package org.dima.dominikschroeck.largestate.Utils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This class contains one static method for computing an arbitrary quantile. Definition as of Wikipedia (DE): https://de.wikipedia.org/wiki/Empirisches_Quantil
 */
public class Quantile {

    public static void main(String[] args) {
        Long ms = TimeUnit.MILLISECONDS.toMillis(1000);
    }

    /**
     * Computes the quantile.
     *
     * @param doubleList   Contains the vdouble values for the quantile computation.
     * @param lowerPercent Sets the quantile in int. E.g. with a lowerPercent of 50 we compute the 50th quantile, i.e. the Median
     * @return
     */
    public static Double quantile(List<Double> doubleList, int lowerPercent) {

        Collections.sort(doubleList);
        int n = doubleList.size();

        double p = (double) (lowerPercent) / 100;


        // 1 Element
        if (n == 1) {
            return doubleList.get(0);

        }


        // WENN np % 1 == 0 DANN Mittelwert der zwei
        if (n * p % 1 == 0) {

            double value_1 = doubleList.get((int) (n * p) - 1);
            double value_2 = doubleList.get((int) (n * p));

            return 0.5 * (value_1 + value_2);
        }


        // WENN n*p nicht ganzzahlig
        else {
            int np = (int) Math.floor(n * p);
            return doubleList.get(np);
        }
    }

}
