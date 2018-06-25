package com.iprogrammer.kafka;


import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Oplog> {

    private final long maxOutOfOrderness = 3500; // 3.5 seconds

    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(Oplog event, long previousElementTimestamp) {
        this.currentMaxTimestamp= Application.getTimeStamp(event);
        return currentMaxTimestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}

/**
 * This generator generates watermarks that are lagging behind processing time by a fixed amount.
 * It assumes that elements arrive in Flink after a bounded delay.
 */
/*
class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<BasicDBObject> {

    private final long maxTimeLag = 5000; // 5 seconds

    @Override
    public long extractTimestamp(BasicDBObject element, long previousElementTimestamp) {
        return  element.getDate("dateTime").getTime();
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current time minus the maximum time lag
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
    }
}*/
