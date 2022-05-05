package com.asardaes.flink.utils;

import com.asardaes.flink.dto.Pojo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

public class PojoTimestampAssigner implements SerializableTimestampAssigner<Pojo> {
    @Override
    public long extractTimestamp(Pojo element, long recordTimestamp) {
        return element.epoch;
    }
}
