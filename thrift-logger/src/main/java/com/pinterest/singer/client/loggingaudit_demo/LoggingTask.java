package com.pinterest.singer.client.loggingaudit_demo;

import com.pinterest.singer.client.ThriftLogger;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class LoggingTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingTask.class);

  private ThriftLogger thriftLogger;
  private int qps;
  private AtomicBoolean cancelled = new AtomicBoolean(false);
  private Thread thread;
  private String name;
  private long seqId = 0;
  private int payloadLen = 10;

  private Class<?> clazz;
  private TFieldIdEnum timestampField;
  private TFieldIdEnum seqIdField;
  private TFieldIdEnum payloadField;
  private TFieldIdEnum loggingAuditHeadersField;


  public LoggingTask(String name, ThriftLogger thriftLogger, Class<?> clazz, int qps, int payloadLen){

     this.name = name;
     this.thriftLogger = thriftLogger;
     this.qps = qps;
     this.clazz = clazz;
     this.payloadLen = payloadLen;
  }

  public void init(){
    try {
      Map m = (Map) clazz.getDeclaredField("metaDataMap").get(null);
      for (Object o : m.keySet()) {
        TFieldIdEnum tf = (TFieldIdEnum) o;
        switch(tf.getFieldName()){
          case "timestamp": this.timestampField = tf; break;
          case "seqId": this.seqIdField = tf; break;
          case "payload": this.payloadField = tf; break;
          case "loggingAuditHeaders": this.loggingAuditHeadersField = tf; break;
          default: throw new Exception("Unexpected field in this class");
        }
      }
      if (timestampField == null || seqIdField == null || payloadField == null){
        throw new Exception("Thrift class field is not correctly initialized.");
      }
    } catch (Exception e){
      LOG.error("Init error for {}", clazz.getName());
      System.exit(1);
    }
  }

  public TBase createThriftMessage(){

    byte[] paylaod = RandomStringUtils.randomAlphabetic(payloadLen).getBytes();
    Preconditions.checkArgument(paylaod.length == payloadLen);

    TBase message = null;
    try {
      message = (TBase) clazz.newInstance();
      long timestamp = System.currentTimeMillis();
      message.setFieldValue(timestampField, timestamp);
      message.setFieldValue(seqIdField, seqId);
      message.setFieldValue(payloadField, ByteBuffer.wrap(paylaod));
      seqId += 1;
    } catch(Exception e){
      LOG.error("thrift message creation error for {}", clazz.getName(), e);
    }
    return message;
  }

  public void run(){
    while(!cancelled.get()) {
       try{
         long start = System.currentTimeMillis();
         long end = start + 1000;
         int i = 0;
         while (System.currentTimeMillis() < end && i < qps){
           i++;
           thriftLogger.append(createThriftMessage());
         }
         long now = System.currentTimeMillis();
         if (end - now > 0) {
           Thread.sleep(end - now);
         }
       }catch (Exception e){
         LOG.error("Writing task {} get interrupted and exit the loop.", name, e);
         break;
       }
    }
  }

  public synchronized void start() {
    if(this.thread == null) {
      init();
      thread = new Thread(this);
      thread.setDaemon(true);
      thread.setName(name);
      thread.start();
      LOG.warn("[{}] created {} and let it write thrift message to file.", Thread.currentThread().getName(), name);
    }
  }

  public synchronized void stop(){
    this.cancelled.set(true);
    if(this.thread != null && thread.isAlive()) {
      thread.interrupt();
    }
    LOG.info("[{}] stopped LoggingTask {}.", Thread.currentThread().getName(), name);
  }
}
