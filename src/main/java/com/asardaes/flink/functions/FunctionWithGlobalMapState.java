package com.asardaes.flink.functions;

import com.asardaes.flink.dto.Pojo;
import com.asardaes.flink.dto.StringList;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class FunctionWithGlobalMapState extends ProcessWindowFunction<Pojo, StringList, String, TimeWindow> {
    private final long windowSizeMs;

    private transient MapStateDescriptor<Long, List<String>> msd;

    public FunctionWithGlobalMapState(long windowSizeMs) {
        this.windowSizeMs = windowSizeMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        msd = new MapStateDescriptor<>(
                "GlobalState#MSD",
                TypeInformation.of(Long.class),
                TypeInformation.of(new TypeHint<>() {})
        );

        msd.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(1L))
                .cleanupInRocksdbCompactFilter(1000L)
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build());
    }

    @Override
    public void process(String s, ProcessWindowFunction<Pojo, StringList, String, TimeWindow>.Context context, Iterable<Pojo> elements, Collector<StringList> out) throws Exception {
        MapState<Long, List<String>> mapState = context.globalState().getMapState(msd);
        long windowStartInclusive = Instant.ofEpochMilli(context.window().maxTimestamp())
                .minusMillis(windowSizeMs)
                .plusMillis(1L)
                .toEpochMilli();

        List<Long> staleKeys = new ArrayList<>();
        for (Long key : mapState.keys()) {
            if (key != null && key < windowStartInclusive - windowSizeMs) {
                staleKeys.add(key);
            }
        }
        for (Long staleKey : staleKeys) {
            mapState.remove(staleKey);
        }

        List<String> values = new ArrayList<>();
        for (Pojo pojo : elements) {
            values.add(pojo.value);
        }

        if (!values.isEmpty()) {
            mapState.put(windowStartInclusive, values);
            out.collect(new StringList(values));
        }
    }
}
