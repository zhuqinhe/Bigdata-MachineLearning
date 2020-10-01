package com.flume.source.delayed_sending;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MyFlumeSource  extends AbstractSource implements Configurable, EventDrivenSource {
    // process info
    private SourceCounter sourceCounter;
    private MySourceEventReader reader;
    private ScheduledExecutorService executor;
    private int intervalMillis;

    public MyFlumeSource() {
    }

    @Override
    public synchronized void configure(Context context) {
        //根据Context读取配置，Context会自动加载flume启动时指定的配置

        // 读配置文件间隔时间，默认值100ms
        intervalMillis = context.getInteger("intervalMillis", 100);
    }

    @Override
    public synchronized void start() {
        // 初始化
        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
        executor = Executors.newSingleThreadScheduledExecutor();
        reader = new MySourceEventReader();
        ChannelProcessor channelProcessor=super.getChannelProcessor();
        // 每个2s执行一次
        Runnable runner = new MyReaderRunnable(reader, sourceCounter,channelProcessor);
        executor.scheduleWithFixedDelay(runner, 0, 2, TimeUnit.MILLISECONDS);

        super.start();
        sourceCounter.start();
    }

    @Override
    public synchronized void stop() {
        executor.shutdown();
        try {
            executor.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        executor.shutdownNow();
        super.stop();
        sourceCounter.stop();
    }

    private class MyReaderRunnable implements Runnable {

        private MySourceEventReader reader;
        private SourceCounter sourceCounter;
        private ChannelProcessor channelProcessor;
        public MyReaderRunnable(MySourceEventReader reader, SourceCounter sourceCounter,ChannelProcessor channelProcessor) {
            this.reader = reader;
            this.sourceCounter = sourceCounter;
            this.channelProcessor=channelProcessor;
        }

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                // 读事件
                List<Event> events = reader.readEvents(5);
                // 提交
                sourceCounter.addToEventReceivedCount(events.size());
                sourceCounter.incrementAppendBatchReceivedCount();
                channelProcessor.processEventBatch(events);
                // sleep intervalMillis
                try {
                    Thread.sleep(intervalMillis);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}