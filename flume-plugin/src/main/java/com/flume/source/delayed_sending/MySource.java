package com.flume.source.delayed_sending;

import com.flume.interceptor.filter_body.ETLInterceptor;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class MySource extends AbstractSource implements Configurable, PollableSource {
    private static Logger logger = LoggerFactory.getLogger(MySource.class);
    // 定义配置文件将来要读取的字段
    private Long delay;
    private String field;

    // 初始化配置信息
    @Override
    public void configure(Context context) {
        delay = context.getLong("delay");
        field = context.getString("field", "Hoob");
    }

    @Override
    public Status process() throws EventDeliveryException {
        try {
            // 创建事件头信息
            HashMap<String, String> hearderMap = new HashMap<>();
            // 创建事件
            SimpleEvent event = new SimpleEvent();
            // 循环封装事件
            for (int i = 0; i < 5; i++) {
                // 给事件设置头信息
                event.setHeaders(hearderMap);
                // 给事件设置内容
                event.setBody((field + i).getBytes());
                // 将事件写入 channel
                getChannelProcessor().processEvent(event);
                Thread.sleep(delay);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}