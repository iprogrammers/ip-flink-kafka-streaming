package com.iprogrammer.kafka;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class OrderTheRecords extends ProcessWindowFunction<Oplog, Oplog, Long, TimeWindow> {

    @Override
    public void process(Long s, Context context, Iterable<Oplog> iterable, Collector<Oplog> collector) throws Exception {
        for (Oplog oplog : iterable) {
            collector.collect(oplog);
        }
    }
}