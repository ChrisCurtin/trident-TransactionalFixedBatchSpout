package com.curtinhome.com.trident.examples;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class SumUpdater extends BaseStateUpdater<SumDB> {
    public void updateState(SumDB a_state, List<TridentTuple> a_tuples, TridentCollector a_collector) {
        List<String> words = new ArrayList<String>();
        for (TridentTuple tuple: a_tuples) {
            System.out.println("Got a Tuple: " + tuple);
            words.add(tuple.getString(0));
        }

        a_state.save(words);
    }
}
