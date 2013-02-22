package com.curtinhome.com.trident.examples;

import storm.trident.state.State;

import java.util.List;

public class SumDB implements State {
    public void beginCommit(Long a_txid) {
        long val = a_txid;

    }
    public void commit(Long a_txid) {
        long val = a_txid;
    }

    public void save(List<String> a_data) {

        List<String> a_items = a_data;
    }
}

