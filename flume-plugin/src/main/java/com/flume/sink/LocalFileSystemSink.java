/**
 * 
 */
package com.flume.sink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.common.base.Preconditions;


public class LocalFileSystemSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(LocalFileSystemSink.class);

  private static final int DFLT_BATCH_SIZE = 100;
  private static final int DFLT_LOG_EVERY_N_EVENTS = 10000;

  private CounterGroup counterGroup;
  private int batchSize = DFLT_BATCH_SIZE;
  private int logEveryNEvents = DFLT_LOG_EVERY_N_EVENTS;
  private String path;

  public LocalFileSystemSink() {

      counterGroup = new CounterGroup();
  }

  @Override
  public void configure(Context context) {
    batchSize = context.getInteger("batchSize", DFLT_BATCH_SIZE);
    this.path = context.getString("path");
    logger.debug(this.getName() + " " + "batch size set to " + String.valueOf(batchSize));
    Preconditions.checkArgument(batchSize > 0, "Batch size must be > 0");

    logEveryNEvents = context.getInteger("logEveryNEvents", DFLT_LOG_EVERY_N_EVENTS);
    logger.debug(this.getName() + " " + "log event N events set to " + logEveryNEvents);
    Preconditions.checkArgument(logEveryNEvents > 0, "logEveryNEvents must be > 0");
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;

    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    Event event = null;
    long eventCounter = counterGroup.get("events.success");
    Map<String,PrintWriter> outMap = new HashMap<String,PrintWriter>();  
    try {
      transaction.begin();
      int i = 0;
      
      for (i = 0; i < batchSize; i++) {
        event = channel.take();
        
        if (++eventCounter % logEveryNEvents == 0) {
            logger.info("LocalFileSystem sink {} successful processed {} events.", getName(), eventCounter);
        }
        if (event == null) {
            status = Status.BACKOFF;
            break;
        }
        
        String log = new String(event.getBody(),Charset.forName("utf-8"));        
        String fileName = event.getHeaders().get("fileName");
        
        String file = this.path +  fileName;
        file = this.getWriteFile(file, event.getHeaders());
        File wFile = new File(file);
        if(!wFile.exists()){
        	wFile.mkdirs();
        	if(wFile.isDirectory()){
        		wFile.delete();
        	}
        	wFile.createNewFile();
        }
        PrintWriter pw = outMap.get(file);
        if(null==pw){
        	pw = new  PrintWriter(new FileOutputStream(new File(file), true));
        	outMap.put(file, pw);
        }
        pw.println(log);        
      }     
      transaction.commit();
      counterGroup.addAndGet("events.success", (long) Math.min(batchSize, i));
      counterGroup.incrementAndGet("transaction.success");
    } catch (FileNotFoundException ex) {
      transaction.rollback();
      counterGroup.incrementAndGet("transaction.failed");
      logger.error("Failed to deliver event. Exception follows.", ex);
      throw new EventDeliveryException("Failed to deliver event: " + event, ex);
    }catch (IOException ex) {
        transaction.rollback();
        counterGroup.incrementAndGet("transaction.failed");
        logger.error("Failed to deliver event. Exception follows.", ex);
        throw new EventDeliveryException("Failed to deliver event: " + event, ex);
    }catch (Exception ex) {
        transaction.rollback();
        counterGroup.incrementAndGet("transaction.failed");
        logger.error("Failed to deliver event. Exception follows.", ex);
        throw new EventDeliveryException("Failed to deliver event: " + event, ex);
    }finally {
    	this.closePrintWriter(outMap);
    	transaction.close();
    }

    return status;
  }
  
  
  
  	private void closePrintWriter(Map<String,PrintWriter> outMap){
  		if(null==outMap) {
            return;
        }
  		for(String key : outMap.keySet()){
  			outMap.get(key).close();
  		}
  	}
  

	private  String getWriteFile(String file,Map<String,String> header) {
		if(StringUtils.isEmpty(file)) {
            return file;
        }
		if(null==header) {
            return file;
        }
		
		String regex = "\\%\\{([^\\}]*)\\}";
		java.util.regex.Pattern pattern = Pattern.compile(regex);
		//String url = "http://www.sina.com.cn/abc.hls?starttime=a&endtime=b&xx=x";
		Matcher matcher = pattern.matcher(file);	
		
		if(matcher.find()){			
			String key  = matcher.group(1);
			String value = header.get(key);
			if(StringUtils.isEmpty(value)){
				file = file.replace("%{"+key+"}", "");
				return getWriteFile(file,header); 
			}else{
				file = file.replace("%{"+key+"}", value);
				return getWriteFile(file,header);
			}			
		}else{
			return file;
		}	
	}
  

  @Override
  public void start() {
    logger.info("Starting {}...", this);

    counterGroup.setName(this.getName());
    super.start();

    logger.info("Null sink {} started.", getName());
  }

  @Override
  public void stop() {
    logger.info("Null sink {} stopping...", getName());

    super.stop();

    logger.info("Null sink {} stopped. Event metrics: {}",
        getName(), counterGroup);
  }

  @Override
  public String toString() {
    return "NullSink " + getName() + " { batchSize: " + batchSize + " }";
  }

}
