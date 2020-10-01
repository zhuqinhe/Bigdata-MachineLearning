package com.flume.interceptor.filter_body;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class ETLInterceptor implements Interceptor{
    private static Logger logger = LoggerFactory.getLogger(ETLInterceptor.class);
    @Override  
    public void initialize() {  
  
    }  
    @Override  
    public Event intercept(Event event) {
        String body = new String(event.getBody(), Charsets.UTF_8);
        String newBody=body;  
        try{  
           //add begin
            newBody=body.replaceAll(",","");
           //add end
            event.setBody(newBody.toString().getBytes());  
        }catch (Exception e){  
            logger.warn(body,e);  
            event=null;  
        }  
        return event;  
    }  
  
    @Override  
    public List<Event> intercept(List<Event> events) {
       /* for (Event event : list) {
            intercept(event);
        }
        return list;*/
       //亦可做自己的逻辑处理
        List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());
        for (Event event : events) {  
            Event interceptedEvent = intercept(event);  
            if (interceptedEvent != null) {  
                intercepted.add(interceptedEvent);  
            }  
        }  
        return intercepted;  
    }  
  
    @Override  
    public void close() {  
  
    }  
  
    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }
        @Override  
        public void configure(Context context) {
  
        }  
    }  
}  
