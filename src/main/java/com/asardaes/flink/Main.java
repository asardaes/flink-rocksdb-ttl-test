package com.asardaes.flink;

import com.asardaes.flink.dto.Pojo;
import com.asardaes.flink.functions.FunctionWithGlobalListState;
import com.asardaes.flink.functions.FunctionWithGlobalMapState;
import com.asardaes.flink.functions.FunctionWithWindowListState;
import com.asardaes.flink.input.TestInputFormat;
import com.asardaes.flink.utils.PojoKeySelector;
import com.asardaes.flink.utils.PojoTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.getConfig().setAutoWatermarkInterval(1000L);

        DataStreamSource<Pojo> source = execEnv.createInput(new TestInputFormat(50L))
                .setParallelism(1);

        PojoKeySelector keySelector = new PojoKeySelector();
        WatermarkStrategy<Pojo> watermarkStrategy = WatermarkStrategy.<Pojo>forMonotonousTimestamps()
                .withTimestampAssigner(new PojoTimestampAssigner());

        DataStream<Pojo> timestamped = source
                .keyBy(keySelector)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .setParallelism(4)
                .name("timestamped")
                .uid("timestamped");

        long windowSizeSeconds = 11L;

        WindowedStream<Pojo, String, TimeWindow> windowed = DataStreamUtils.reinterpretAsKeyedStream(timestamped, keySelector)
                .window(SlidingEventTimeWindows.of(
                        Time.seconds(windowSizeSeconds),
                        Time.seconds(1L)
                ))
                .allowedLateness(Time.seconds(windowSizeSeconds));

        windowed
                .process(new FunctionWithWindowListState())
                .name("window-list-state")
                .uid("window-list-state")
                .addSink(new DiscardingSink<>())
                .setParallelism(1)
                .name("sink1")
                .uid("sink1");

        windowed
                .process(new FunctionWithGlobalMapState(windowSizeSeconds * 1000L))
                .name("global-map-state")
                .uid("global-map-state")
                .addSink(new DiscardingSink<>())
                .setParallelism(1)
                .name("sink2")
                .uid("sink2");

        windowed
                .process(new FunctionWithGlobalListState(windowSizeSeconds * 1000L))
                .name("global-list-state")
                .uid("global-list-state")
                .addSink(new DiscardingSink<>())
                .setParallelism(1)
                .name("sink3")
                .uid("sink3");

        execEnv.execute();
    }
}
