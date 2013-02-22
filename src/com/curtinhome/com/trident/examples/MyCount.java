package com.curtinhome.com.trident.examples;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class MyCount implements CombinerAggregator<Long> {

    @Override
    public Long init(TridentTuple tuple) {
        return 1L;
    }

    @Override
    public Long combine(Long val1, Long val2) {
        return val1 + val2;
    }

    @Override
    public Long zero() {
        return 0L;
    }

}