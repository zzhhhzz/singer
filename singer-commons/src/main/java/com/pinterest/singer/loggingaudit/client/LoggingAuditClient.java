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
import com.pinterest.singer.loggingaudit.client.utils.CommonUtils;
import com.pinterest.singer.loggingaudit.client.utils.ConfigUtils;
import com.pinterest.singer.loggingaudit.client.utils.KafkaUtils;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditEvent;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditHeaders;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditStage;
import com.pinterest.singer.loggingaudit.thrift.configuration.LoggingAuditClientConfig;
import com.pinterest.singer.loggingaudit.thrift.configuration.LoggingAuditEventSenderConfig;
import com.pinterest.singer.loggingaudit.thrift.configuration.AuditConfig;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LoggingAuditClient {

  private static Logger LOG = LoggerFactory.getLogger(LoggingAuditClient.class);
  private final LoggingAuditStage stage;
  private final String host;
  private final String KAFKA_SENDER_NAME = "AuditEventKafkaSender";
  private LoggingAuditEventSender sender;
  private LinkedBlockingDeque<LoggingAuditEvent> queue;
  private LoggingAuditEventGenerator loggingAuditEventGenerator;
  private LoggingAuditClientConfig config;
  private int enqueueWaitInMilliseconds = 0;
  private boolean enableAuditForAllTopicsByDefault = false;
  private ConcurrentHashMap<String, AuditConfig> auditConfigs = new ConcurrentHashMap<>();
  private AtomicBoolean enqueueEnabled = new AtomicBoolean(true);

  public LoggingAuditClient(LoggingAuditClientConfig config){
    this(CommonUtils.getHostName(), config);
  }

  public LoggingAuditClient(String host, LoggingAuditClientConfig config){
    this.host = host;
    this.config = config;
    this.queue = new LinkedBlockingDeque<>(config.getQueueSize());
    this.enqueueWaitInMilliseconds = config.enqueueWaitInMilliseconds;
    this.enableAuditForAllTopicsByDefault = config.isEnableAuditForAllTopicsByDefault();
    this.stage = config.getStage();
    this.auditConfigs.putAll(config.getAuditConfigs());
    this.loggingAuditEventGenerator = new LoggingAuditEventGenerator(this.host, this.stage, this.auditConfigs);
    initLoggingAuditEventSender(this.config.getSenderConfig(),this.queue);
  }


  public void initLoggingAuditEventSender(LoggingAuditEventSenderConfig config, LinkedBlockingDeque<LoggingAuditEvent> queue){
    this.sender = new AuditEventKafkaSender(config.getKafkaSenderConfig(), queue , this.stage, this.host, KAFKA_SENDER_NAME);
    ((AuditEventKafkaSender) this.sender).setKafkaProducer(KafkaUtils.createKafkaProducer(config.getKafkaSenderConfig().getKafkaProducerConfig(), KAFKA_SENDER_NAME));
    this.sender.start();
  }


  public LoggingAuditClientConfig getConfig() {
    return config;
  }

  public ConcurrentHashMap<String, AuditConfig> getAuditConfigs() {
    return auditConfigs;
  }

  /**
   *  create and enqueue a LoggingAuditEvent if TopicAuditConfig exists for this topic/logName,
   *  enqueueEnabled is true and there is capacity available in the queue.
   * @param loggingAuditHeaders
   * @param isMessageValid
   * @param messageAcknowledgedTimestamp
   */
  public void audit(String loggingAuditName, LoggingAuditHeaders loggingAuditHeaders,
                    boolean isMessageValid, long messageAcknowledgedTimestamp){
    audit(loggingAuditName, loggingAuditHeaders, isMessageValid, messageAcknowledgedTimestamp, "","");
  }

  /**
   *  create and enqueue a LoggingAuditEvent if TopicAuditConfig exists for this topic/logName,
   *  enqueueEnabled is true and there is capacity available in the queue.
   * @param loggingAuditName
   * @param loggingAuditHeaders
   * @param isMessageValid
   * @param messageAcknowledgedTimestamp
   * @param kafkaCluster
   * @param topic
   */
  public void audit(String loggingAuditName, LoggingAuditHeaders loggingAuditHeaders,
                    boolean isMessageValid,
                    long messageAcknowledgedTimestamp, String kafkaCluster, String topic){
     if (!this.auditConfigs.containsKey(loggingAuditName)){
        OpenTsdbMetricConverter.incr(LoggingAuditClientMetrics.AUDIT_EVENT_WITHOUT_TOPIC_CONFIGURED_ERROR, "logName=" + loggingAuditHeaders.getLogName(), "host=" +  host, "stage=" + stage.toString());
        return;
     }
     if (enqueueEnabled.get()){
         LoggingAuditEvent loggingAuditEvent =  loggingAuditEventGenerator.generateAuditEvent(
             loggingAuditName, loggingAuditHeaders, isMessageValid, messageAcknowledgedTimestamp,
             kafkaCluster, topic);
         boolean successful = false;
         try{
           // compared to put() which is blocking until available space in the queue, offer()
           // returns false for enqueuing object if there is still no space after waiting for the
           // specified the time period.
           successful = queue.offer(loggingAuditEvent, enqueueWaitInMilliseconds, TimeUnit.MILLISECONDS);
           } catch (InterruptedException e){
           OpenTsdbMetricConverter.incr(LoggingAuditClientMetrics.AUDIT_CLIENT_ENQUEUE_EXCEPTION,  "host=" +  host, "stage=" + stage.toString());
         }
         if (successful){
           int qSize = queue.size();
           double qUsagePercent = qSize * 1.0 / (qSize + queue.remainingCapacity());
           OpenTsdbMetricConverter.gauge(LoggingAuditClientMetrics.AUDIT_CLIENT_QUEUE_SIZE, qSize,"host=" +  host, "stage=" + stage.toString());
           OpenTsdbMetricConverter.gauge(LoggingAuditClientMetrics.AUDIT_CLIENT_QUEUE_USAGE_PERCENT, qUsagePercent, "host=" +  host, "stage=" + stage.toString());
           OpenTsdbMetricConverter.incr(LoggingAuditClientMetrics.AUDIT_CLIENT_ENQUEUE_EVENTS_ADDED, "logName=" + loggingAuditHeaders.getLogName(), "host=" +  host, "stage=" + stage.toString());
         } else {
           OpenTsdbMetricConverter.incr(LoggingAuditClientMetrics.AUDIT_CLIENT_ENQUEUE_EVENTS_DROPPED, "logName=" + loggingAuditHeaders.getLogName(), "host=" +  host, "stage=" + stage.toString());
         }
     } else{
       OpenTsdbMetricConverter.incr(LoggingAuditClientMetrics.AUDIT_CLIENT_ENQUEUE_DISABLED_EVENTS_DROPPED, "logName=" + loggingAuditHeaders.getLogName(), "host=" +  host, "stage=" + stage.toString());
     }
  }

  public AuditConfig addAuditConfigFromKVs(String loggingAuditName, Map<String, String> properties) {
    try {
      AuditConfig auditConfig = ConfigUtils.createAuditConfigFromKVs(properties);
      return addAuditConfig(loggingAuditName, auditConfig);
    } catch (ConfigurationException e){
      LOG.error("[{}] couldn't create TopicAuditConfig for {}.", Thread.currentThread().getName(), loggingAuditName);
      return null;
    }
  }

  public AuditConfig addAuditConfig(String loggingAuditName, AuditConfig auditConfig){
    if (auditConfig != null){
      this.auditConfigs.put(loggingAuditName, auditConfig);
      LOG.info("[{}] add TopicAuditConfig ({}) for {}.", Thread.currentThread().getName(),
          auditConfig.toString(), loggingAuditName);
    }
    return auditConfig;
  }

  public boolean auditConfigExists(String loggingAuditName){
    return this.auditConfigs.containsKey(loggingAuditName);
  }

  public void close(){
     enqueueEnabled.set(false);
     LOG.info("[{}] set enqueueEnabled to false and no more LoggingAudit events will be added to queue while closing LoggingAuditClient.", Thread.currentThread().getName());
     this.sender.stop();
     LOG.info("[{}] stopped the LoggingAuditSender.", Thread.currentThread().getName());
  }
}
