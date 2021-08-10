package com.nec.spark.executor;

import java.util.ServiceLoader;

public interface SparkNecExecutorControl {
    void init();

    void stop();

    static SparkNecExecutorControl getInstance() {
        return ServiceLoader.load(SparkNecExecutorControl.class).iterator().next();
    }
}

