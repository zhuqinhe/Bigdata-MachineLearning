package com.flume.interceptor.select_sink;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * @author zhuqinhe
 * 在flume的安装目录下的plugins.d 目录下新建文件夹ETLInterceptor.文件夹这种新建三个文件夹lib，libext，native。
 * 将jar包放入lib文件夹中
 * Flume 拓扑结构中的 Multiplexing 结构，Multiplexing的原理是，
 * 根据 event 中 Header 的某个 key 的值，将不同的 event 发送到不同的 Channel中，
 * 所以我们需要自定义一个 Interceptor，为不同类型的 event 的 Header 中的 key 赋予 不同的值。
 * 这里以端口数据模拟日志，以数字（单个）和字母（单个）模拟不同类型的日志，
 * 需要自定义 interceptor 区分数字和字母，将其分别发往不同的分析系统（Channel）
 *
 */
public class CustomInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 实现字母和数字分别发往不同的sink
     * */
    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        if (body[0] < 'z' && body[0] > 'a') {
            // 自定义头信息
            event.getHeaders().put("type", "letter");
        } else if (body[0] > '0' && body[0] < '9') {
            // 自定义头信息
            event.getHeaders().put("type", "number");
        }
        return event;
    }

    /***批量事件拦截****/
    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new CustomInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }
}