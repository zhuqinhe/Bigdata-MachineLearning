package com.flume.source.delayed_sending;

import com.google.common.collect.Lists;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.event.EventBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public class MySourceEventReader   {
    private final Charset outputCharset = Charset.forName("UTF-8");

    public Event readEvent() {
        /**
         * 发送单独的一个event，内容为test
         */
        return EventBuilder.withBody("test", outputCharset);
    }


    public List<Event> readEvents(int numEvents) {
        /**
         * 发送多个Event列表
         */
        List<Event> retList = Lists.newLinkedList();
        for (int i = 0; i < numEvents; ++i) {
            retList.add(EventBuilder.withBody("test", outputCharset));
        }
        return retList;
    }
}