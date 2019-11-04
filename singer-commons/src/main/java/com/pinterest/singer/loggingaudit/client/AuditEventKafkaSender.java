/**
 * Copyright 2019 Pinterest, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.singer.loggingaudit.client;

import com.pinterest.singer.loggingaudit.client.common.LoggingAuditClientMetrics;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditHeaders;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditEvent;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditStage;
import com.pinterest.singer.loggingaudit.thrift.configuration.KafkaSenderConfig;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AuditEventKafkaSender implements LoggingAuditEventSender {

  private static final Logger LOG = LoggerFactory.getLogger(AuditEventKafkaSender.class);

  private final LoggingAuditStage stage;
  private final String host;
  private final LinkedBlockingDeque<LoggingAuditEvent> queue;
  private KafkaProducer<byte[], byte[]> kafkaProducer;
  private TSerializer serializer = new TSerializer();
  private AtomicBoolean cancelled = new AtomicBoolean(false);
  private String topic;
  private String name;
  private Thread thread;
  private int dequeueWaitInSeconds = 30;
  private int stopGracePeriodInSeconds = 300;
  private int callingThreadSleepInSeconds = 30;
  private int numOfPartitionsToTrySending = 3;
  private Map<Integer, AtomicInteger> partitionErrorCounts = new ConcurrentHashMap<>();
  private Map<LoggingAuditHeaders, Set<Integer>> eventBadPartitionsMap = new ConcurrentHashMap<>();

  public AuditEventKafkaSender(KafkaSenderConfig config,
                               LinkedBlockingDeque<LoggingAuditEvent> queue,
                               LoggingAuditStage stage , String host, String name){
     this.topic = config.getTopic();
     this.queue = queue;
     this.stage = stage;
     this.host = host;
     this.name = name;
  }

  public KafkaProducer<byte[], byte[]> getKafkaProducer() {
    return kafkaProducer;
  }

  public void setKafkaProducer(KafkaProducer<byte[], byte[]> kafkaProducer) {
    this.kafkaProducer = kafkaProducer;
  }


  public int getGoodPartition(Set<Integer> badPartitions, int numOfPartitions){
    int randomPartition = 0;
    int trials = 10;
    while(trials > 0){
      trials -= 1;
      randomPartition = ThreadLocalRandom.current().nextInt(numOfPartitions);
      if (!badPartitions.contains(randomPartition)){
        break;
      }
    }
    return randomPartition;
  }

  @Override
  public void run() {
    LoggingAuditEvent event = null;
    ProducerRecord<byte[], byte[]> record;
    byte[] value = null;
    List<PartitionInfo> partitionInfoList = this.kafkaProducer.partitionsFor(topic);
    LOG.info("Partitions of {}: {}", topic, partitionInfoList);
    for(PartitionInfo partitionInfo : partitionInfoList){
      partitionErrorCounts.put(partitionInfo.partition(), new AtomicInteger(0));
    }
     while(!cancelled.get()){
       try {
         event = queue.poll(dequeueWaitInSeconds, TimeUnit.SECONDS);
         if (event != null){
           final LoggingAuditEvent currEvent = event;
           final LoggingAuditHeaders loggingAuditHeaders = event.getLoggingAuditHeaders();
           try {
             value = serializer.serialize(event);
             if (!eventBadPartitionsMap.containsKey(loggingAuditHeaders)) {
               record = new ProducerRecord<>(this.topic, value);
             } else {
               record = new ProducerRecord<>(this.topic, getGoodPartition(eventBadPartitionsMap.get(loggingAuditHeaders), partitionInfoList.size()), null, value);
             }
             kafkaProducer.send(record, new Callback() {
               @Override
               public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                 if (e != null) {
                   int partition = recordMetadata.partition();
                   partitionErrorCounts.get(partition).incrementAndGet();
                   OpenTsdbMetricConverter.incr(LoggingAuditClientMetrics.AUDIT_CLIENT_SENDER_KAFKA_PARTITION_ERROR, 1,
                       "host=" +  host, "stage=" + stage.toString(), "topic=" + topic, "partition=" + partition);

                   if (!eventBadPartitionsMap.containsKey(loggingAuditHeaders)){
                      eventBadPartitionsMap.put(loggingAuditHeaders, ConcurrentHashMap.newKeySet());
                     OpenTsdbMetricConverter.gauge(LoggingAuditClientMetrics.AUDIT_CLIENT_SENDER_KAFKA_EVENTS_RETRIED, eventBadPartitionsMap.size(),
                         "host=" +  host, "stage=" + stage.toString(), "topic=" + topic);
                   }

                   eventBadPartitionsMap.get(loggingAuditHeaders).add(partition);
                   if (eventBadPartitionsMap.get(loggingAuditHeaders).size() >= numOfPartitionsToTrySending){
                     LOG.debug("Failed to send audit event after trying these partitions {}. Drop this event.", eventBadPartitionsMap.get(loggingAuditHeaders));

                     OpenTsdbMetricConverter.incr(LoggingAuditClientMetrics.AUDIT_CLIENT_SENDER_KAFKA_EVENTS_DROPPED, 1,
                         "host=" +  host, "stage=" + stage.toString(), "logName=" + loggingAuditHeaders.getLogName(), "dropCode=" );

                     eventBadPartitionsMap.remove(loggingAuditHeaders);

                   } else {
                     boolean success = false;
                     try {
                        success = queue.offerFirst(currEvent, 3, TimeUnit.SECONDS);
                        if (!success){
                          LOG.debug("Failed to enqueue LoggingAuditEvent at head of the queue when executing producer send callback. Drop this event.");
                          eventBadPartitionsMap.remove(loggingAuditHeaders);
                        }
                     } catch(InterruptedException ex){
                       LOG.debug("Enqueuing LoggingAuditEvent at head of the queue was interrupted in callback. Drop this event");
                       eventBadPartitionsMap.remove(loggingAuditHeaders);
                     }
                   }
                 } else {
                   OpenTsdbMetricConverter.incr(LoggingAuditClientMetrics.AUDIT_CLIENT_SENDER_KAFKA_EVENTS_ACKED, 1,
                       "host=" +  host, "stage=" + stage.toString(), "logName=" + loggingAuditHeaders.getLogName());
                   eventBadPartitionsMap.remove(loggingAuditHeaders);
                 }
               }
             });
           }catch(TException e){
             LOG.debug("[{}] failed to construct ProducerRecord because of serialization exception.", Thread.currentThread().getName(), e);
             OpenTsdbMetricConverter.incr(LoggingAuditClientMetrics.AUDIT_CLIENT_SENDER_SERIALIZATION_EXCEPTION, 1,
                 "host=" +  host, "stage=" + stage.toString(), "logName=" + loggingAuditHeaders.getLogName());
             eventBadPartitionsMap.remove(loggingAuditHeaders);
           }
         }
       }catch(InterruptedException e){
         LOG.warn("[{}] got interrupted when polling the queue and while loop is ended!", Thread.currentThread().getName(), e);
         OpenTsdbMetricConverter.incr(LoggingAuditClientMetrics.AUDIT_CLIENT_DEQUEUE_EXCEPTION, 1,
             "host=" +  host, "stage=" + stage.toString());
         break;
       }
     }
  }


  public synchronized void start() {
    if(this.thread == null) {
      thread = new Thread(this);
      thread.setDaemon(true);
      thread.setName(name);
      thread.start();
      LOG.warn("[{}] created and started [{}] to let it dequeue LoggingAuditEvents and send to Kafka.", Thread.currentThread().getName(), name);
    }
  }

  /**
   *  reserve some time (300 seconds at most)to let AuditEventKafkaSender to send out
   *  LoggingAuditEvent in the queue and gracefully stop AuditEventKafkaSender.
   */
  public synchronized void stop(){
    LOG.warn("[{}] waits up to {} seconds to let [{}] send out LoggingAuditEvents left in the queue if any.", Thread.currentThread().getName(), stopGracePeriodInSeconds, name);
    int i = 0;
    int numOfRounds = stopGracePeriodInSeconds / callingThreadSleepInSeconds;
    while (queue.size() > 0 && this.thread != null && thread.isAlive() && i < numOfRounds){
      i += 1;
      try{
        Thread.sleep(callingThreadSleepInSeconds * 1000);
        int qSize = queue.size();
        double qUsagePercent = qSize * 1.0 / (qSize + queue.remainingCapacity());
        OpenTsdbMetricConverter.gauge(LoggingAuditClientMetrics.AUDIT_CLIENT_QUEUE_SIZE, qSize,"host=" +  host, "stage=" + stage.toString());
        OpenTsdbMetricConverter.gauge(LoggingAuditClientMetrics.AUDIT_CLIENT_QUEUE_USAGE_PERCENT, qUsagePercent, "host=" +  host, "stage=" + stage.toString());
        LOG.info("In {} round, [{}] waited {} seconds and the current queue size is {}", i, Thread.currentThread().getName(), callingThreadSleepInSeconds, queue.size());
      }catch(InterruptedException e){
        LOG.warn("[{}] got interrupted while waiting for [{}] to send out LoggingAuditEvents left in the queue.", Thread.currentThread().getName(), name, e);
      }
    }
    cancelled.set(true);
    if(this.thread != null && thread.isAlive()) {
      thread.interrupt();
    }
    try {
      this.kafkaProducer.close();
    } catch (Throwable t) {
      LOG.warn("Exception is thrown while stopping {}.", name,  t);
    }
    LOG.warn("[{}] is stopped and the number of LoggingAuditEvents left in the queue is {}.", name, queue.size());
  }


  public int getDequeueWaitInSeconds() {
    return dequeueWaitInSeconds;
  }

  public void setDequeueWaitInSeconds(int dequeueWaitInSeconds) {
    this.dequeueWaitInSeconds = dequeueWaitInSeconds;
  }

  public int getStopGracePeriodInSeconds() {
    return stopGracePeriodInSeconds;
  }

  public void setStopGracePeriodInSeconds(int stopGracePeriodInSeconds) {
    this.stopGracePeriodInSeconds = stopGracePeriodInSeconds;
  }

  public int getCallingThreadSleepInSeconds() {
    return callingThreadSleepInSeconds;
  }

  public void setCallingThreadSleepInSeconds(int callingThreadSleepInSeconds) {
    this.callingThreadSleepInSeconds = callingThreadSleepInSeconds;
  }
}
