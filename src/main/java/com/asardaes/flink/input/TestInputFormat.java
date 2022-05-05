package com.asardaes.flink.input;

import com.asardaes.flink.dto.Pojo;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public class TestInputFormat implements InputFormat<Pojo, InputSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(TestInputFormat.class);

    private static final List<String> KEYS = List.of(
            "0",
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9"
    );

    private static final List<String> VALUES = List.of(
            "A",
            "B",
            "C",
            "D",
            "E"
    );

    private transient Random random;

    private final long sleep;

    public TestInputFormat(long sleep) {
        this.sleep = sleep;
    }

    @Override
    public void configure(Configuration parameters) {
        // nop
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) {
        return new InputSplit[]{
                new GenericInputSplit(0, 1)
        };
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit split) {
        random = new Random();
    }

    @Override
    public boolean reachedEnd() {
        return false;
    }

    @Override
    public Pojo nextRecord(Pojo reuse) throws IOException {
        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }

        int keyId = random.nextInt(KEYS.size());
        int valId = random.nextInt(VALUES.size());

        return new Pojo(KEYS.get(keyId), VALUES.get(valId));
    }

    @Override
    public void close() {
        // nop
    }
}
