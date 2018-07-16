package com.softcell.streaming.utils;


import com.softcell.streaming.Application;
import com.softcell.streaming.model.Oplog;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Oplog> {

    private static final long MAX_OUT_OF_ORDERNESS = 3500; // 3.5 seconds

    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(Oplog event, long previousElementTimestamp) {
        this.currentMaxTimestamp= Application.getTimeStamp(event);
        return currentMaxTimestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - MAX_OUT_OF_ORDERNESS);
    }
}
