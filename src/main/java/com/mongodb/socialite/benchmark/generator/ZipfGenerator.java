package com.mongodb.socialite.benchmark.generator;

import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.pow;
import static java.lang.Math.random;

public class ZipfGenerator {

    double ZIPFIAN_CONSTANT = 0.99;
    private long items;
    long currentN = 0;
    double currentSum = 0.0;
    double zeta2theta;

    public ZipfGenerator(long items) {
        this.items = items;
        this.zeta2theta = zeta(2,ZIPFIAN_CONSTANT);
        this.currentSum = 0;
        this.currentN = 0;
    }


    long zipf(long n, double theta)
    {
        double alpha = 1 / (1 - theta);
        double zetan = zeta(n, theta);
        double eta =
                (1 - pow(2.0 / n, 1 - theta)) /
                        (1 - zeta2theta / zetan);
        double u = random();
        double uz = u * zetan;
        if (uz < 1.0) return 1;
        if (uz < 1.0 + pow(0.5, theta)) return 2;
        return 1 +
                (int)(n * pow(eta*u - eta + 1, alpha));
    }



    double zeta(long n, double theta) {
        if( n > currentN ) {
            for( long i = currentN; i < n; i++ ) {
                currentSum += 1/(Math.pow(i+1,theta));
                currentN++;
            }
        }
        return currentSum;
    }

    public long nextLong() {
        return nextLong(this.items);
    }
    public long nextLong(long size) {
        return zipf(size, ZIPFIAN_CONSTANT) - 1;
    }

}
