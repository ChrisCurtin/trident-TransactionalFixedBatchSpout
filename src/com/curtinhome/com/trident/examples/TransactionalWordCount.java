package com.curtinhome.com.trident.examples;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.curtinhome.trident.spout.TransactionalFixedBatchSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Testing Transactional topologies in Trident. Borrowed heavily from the WordCount example in Storm
 *
 */
public class TransactionalWordCount {
    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }

    public static TransactionalFixedBatchSpout makeTransactionalSpout() {
        List<List<Values>> data = new ArrayList<List<Values>>();
        List<Values> block1 = new ArrayList<Values>();
        block1.add(new Values(1, "500", "ninety nine"));
        block1.add(new Values(2, "300", "ninety nine nine zero"));
        block1.add(new Values(3, "300", "ninety nine nine one"));
        block1.add(new Values(4, "301", "ninety nine nine one"));
        block1.add(new Values(5, "301", "ninety nine nine one"));
        block1.add(new Values(6, "302", "ninety nine nine nine nine one"));
        block1.add(new Values(7, "302", "ninety nine nine nine nine two"));
        block1.add(new Values(8, "302", "ninety nine nine nine nine one"));
        block1.add(new Values(9, "303", "ninety nine nine nine nine two"));
        block1.add(new Values(10, "303", "ninety nine nine nine three"));
        block1.add(new Values(11, "304", "ninety nine four"));
        block1.add(new Values(12, "304", "ninety nine five"));
        data.add(block1);


        List<Values> block2 = new ArrayList<Values>();
        block2.add(new Values(100, "99000", "ninety nine"));
        block2.add(new Values(101, "79000", "ninety nine nine zero"));
        block2.add(new Values(102, "79100", "ninety nine nine one"));
        block2.add(new Values(103, "79100", "ninety nine nine one"));
        block2.add(new Values(104, "79101", "ninety nine nine nine nine one"));
        block2.add(new Values(105, "79102", "ninety nine nine nine nine two"));
        block2.add(new Values(106, "79100", "ninety nine nine nine nine one"));
        block2.add(new Values(107, "79200", "ninety nine nine nine nine two"));
        block2.add(new Values(108, "79301", "ninety nine nine nine three"));
        block2.add(new Values(109, "79402", "ninety nine four"));
        block2.add(new Values(110, "79500", "ninety nine five"));
        block2.add(new Values(111, "79500", "ninety nine five"));
        block2.add(new Values(112, "79402", "ninety nine four"));
        block2.add(new Values(113, "79500", "ninety nine five"));
        block2.add(new Values(114, "79501", "ninety nine six"));
        block2.add(new Values(115, "79501", "ninety nine seven"));
        data.add(block2);


        List<Values> block3 = new ArrayList<Values>();
        block3.add(new Values(200, "49000", "ninety nine"));
        block3.add(new Values(201, "49000", "ninety nine nine zero"));
        block3.add(new Values(202, "49100", "ninety nine nine one"));
        block3.add(new Values(203, "49100", "ninety nine nine one"));
        block3.add(new Values(204, "49101", "ninety nine nine nine nine one"));
        block3.add(new Values(205, "49102", "ninety nine nine nine nine two"));
        block3.add(new Values(206, "49100", "ninety nine nine nine nine one"));
        block3.add(new Values(207, "49200", "ninety nine nine nine nine two"));
        block3.add(new Values(208, "49301", "ninety nine nine nine three"));
        block3.add(new Values(209, "49402", "ninety nine four"));
        block3.add(new Values(210, "49500", "ninety nine five"));
        block3.add(new Values(211, "49500", "ninety nine five"));
        block3.add(new Values(212, "49402", "ninety nine four"));
        block3.add(new Values(213, "49500", "ninety nine five"));
        block3.add(new Values(214, "49501", "ninety nine six"));
        block3.add(new Values(215, "49501", "ninety nine seven"));
        block3.add(new Values(216, "49301", "ninety nine nine nine three"));
        block3.add(new Values(217, "49402", "ninety nine four"));
        block3.add(new Values(218, "49500", "ninety nine five"));
        block3.add(new Values(219, "49500", "ninety nine five"));
        block3.add(new Values(220, "49402", "ninety nine four"));
        block3.add(new Values(221, "49500", "ninety nine five"));
        block3.add(new Values(222, "49501", "ninety nine six"));
        block3.add(new Values(223, "49501", "ninety nine seven"));
        data.add(block3);


        return new TransactionalFixedBatchSpout(new Fields("code", "source", "sentence"), 5, data);
    }

    public static StormTopology buildPPTopology(LocalDRPC a_drpc) {
        //
           TransactionalFixedBatchSpout spout =  makeTransactionalSpout();
        TridentTopology topology = new TridentTopology();

        TridentState ppState = topology.newStream("PPSpout", spout)
                .parallelismHint(16)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .shuffle()
                .groupBy(new Fields("source", "word"))
                .aggregate(new Fields("code", "source", "word"), new MyCount(), new Fields("counts"))
                .partitionPersist(new SumDBFactory(), new Fields("source", "word", "counts"), new SumUpdater())
                .parallelismHint(16);
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if (args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildPPTopology(drpc));
            for (int i = 0; i < 100; i++) {
                Thread.sleep(1000);
            }
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildPPTopology(null));
        }
    }
}
