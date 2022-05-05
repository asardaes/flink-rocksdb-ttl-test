package com.asardaes.flink.functions;

import com.asardaes.flink.dto.HashAndEpoch;
import com.asardaes.flink.dto.Pojo;
import com.asardaes.flink.dto.StringList;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FunctionWithGlobalListState extends ProcessWindowFunction<Pojo, StringList, String, TimeWindow> {
    private final Random random = new Random();

    private final long windowSizeMs;

    private transient ListState<HashAndEpoch> listState;

    public FunctionWithGlobalListState(long windowSizeMs) {
        this.windowSizeMs = windowSizeMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<HashAndEpoch> lsd = new ListStateDescriptor<>(
                "GlobalState#LSD",
                HashAndEpoch.class
        );
        lsd.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(1L))
                .cleanupInRocksdbCompactFilter(1000L)
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build());
        listState = getRuntimeContext().getListState(lsd);
    }

    @Override
    public void process(String s, ProcessWindowFunction<Pojo, StringList, String, TimeWindow>.Context context, Iterable<Pojo> elements, Collector<StringList> out) throws Exception {
        List<HashAndEpoch> state = new ArrayList<>();
        boolean expired = false;
        for (HashAndEpoch hashAndEpoch : listState.get()) {
            if (hashAndEpoch.epoch <= context.window().maxTimestamp() - (windowSizeMs * 2L)) {
                expired = true;
            } else {
                state.add(hashAndEpoch);
            }
        }

        if (random.nextInt(10) > 3) {
            if (expired) {
                listState.update(state);
            }
            return;
        }

        List<String> values = new ArrayList<>();
        for (Pojo pojo : elements) {
            state.add(new HashAndEpoch(pojo.hashCode(), pojo.epoch));
            values.add(pojo.value);
        }

        listState.update(state);
        out.collect(new StringList(values));
    }
}
