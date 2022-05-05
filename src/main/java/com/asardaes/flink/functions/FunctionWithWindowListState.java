package com.asardaes.flink.functions;

import com.asardaes.flink.dto.Pojo;
import com.asardaes.flink.dto.StringList;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class FunctionWithWindowListState extends ProcessWindowFunction<Pojo, StringList, String, TimeWindow> {
    private final ListStateDescriptor<Integer> lsd = new ListStateDescriptor<>("WindowState#LSD", Integer.class);

    @Override
    public void process(String s, ProcessWindowFunction<Pojo, StringList, String, TimeWindow>.Context context, Iterable<Pojo> elements, Collector<StringList> out) throws Exception {
        List<Integer> hashes = new ArrayList<>();
        List<String> values = new ArrayList<>();

        for (Pojo pojo : elements) {
            hashes.add(pojo.hashCode());
            values.add(pojo.value);
        }

        context.windowState().getListState(lsd).update(hashes);
        out.collect(new StringList(values));
    }

    @Override
    public void clear(ProcessWindowFunction<Pojo, StringList, String, TimeWindow>.Context context) throws Exception {
        super.clear(context);
        context.windowState().getListState(lsd).clear();
    }
}
