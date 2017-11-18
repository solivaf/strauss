package com.hermes.strauss.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StraussApplicationConfig {

    @Value("${strauss.async.max.threads}")
    public void setMaxThreads(String maxThreads) {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", maxThreads);
    }
}
