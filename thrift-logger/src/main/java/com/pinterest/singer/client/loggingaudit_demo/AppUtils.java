package com.pinterest.singer.client.loggingaudit_demo;

import com.pinterest.singer.client.AuditableLogbackThriftLogger;
import com.pinterest.singer.client.ThriftLogger;
import com.pinterest.singer.client.ThriftLoggerConfig;
import com.pinterest.singer.client.ThriftLoggerFactory;
import com.pinterest.singer.client.logback.AuditableLogbackThriftLoggerFactory;

import com.google.common.base.Preconditions;
import org.apache.thrift.TFieldIdEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

public class AppUtils {

  private static final Logger LOG = LoggerFactory.getLogger(AppUtils.class);

  public static final File logDir = new File("/mnt/thrift_logger");


  public static void initThriftLoggerFactory(String loggingAuditClientPath){
    ThriftLoggerFactory.initialize();
    ThriftLoggerFactory.ThriftLoggerFactoryInterface instance = ThriftLoggerFactory.getThriftLoggerFactoryInstance();
    Preconditions.checkArgument( instance.getClass().isAssignableFrom(AuditableLogbackThriftLoggerFactory.class));
    Preconditions.checkArgument(((AuditableLogbackThriftLoggerFactory)instance).getLoggingAuditClient() == null);
    LOG.info("Start to set LoggingAuditClient...");
    ((AuditableLogbackThriftLoggerFactory) instance).createLoggingAuditClientFromConfigFile(loggingAuditClientPath);
    Preconditions.checkNotNull(((AuditableLogbackThriftLoggerFactory)instance).getLoggingAuditClient());
    LOG.info("Logging audit client config:  {}", ((AuditableLogbackThriftLoggerFactory) instance).getLoggingAuditClient().getConfig());
    LOG.info("Topic configs specified in the audit client config file: {}", ((AuditableLogbackThriftLoggerFactory) instance).getLoggingAuditClient().getAuditConfigs());
  }

  public static ThriftLoggerConfig createThriftLoggerConfig(String kafkaTopic,
                                                            int maxRetentionSecs,
                                                            int logRotationThresholdBytes,
                                                            Class<?> thriftClazz,
                                                            boolean enableLoggingAudit,
                                                            double auditSamplingRate ){
    ThriftLoggerConfig thriftLoggerConfig = new ThriftLoggerConfig(logDir, kafkaTopic,maxRetentionSecs,logRotationThresholdBytes, thriftClazz, enableLoggingAudit, auditSamplingRate);
    return thriftLoggerConfig;
  }

  public static ThriftLogger createThriftLogger(ThriftLoggerConfig thriftLoggerConfig){
    LOG.info("creating Thrift Logger based on config: {}", thriftLoggerConfig);
    ThriftLogger thriftLogger =  ThriftLoggerFactory.getLogger(thriftLoggerConfig);
     Preconditions.checkArgument(thriftLogger.getClass().isAssignableFrom(AuditableLogbackThriftLogger.class));
    return thriftLogger;
  }

  public static LoggingTask createAndStartLoggingTask(String taskName, ThriftLogger logger, Class<?> dataClassName, int qps, int payloadLen){
    LoggingTask task = new LoggingTask(taskName,logger, dataClassName,qps,payloadLen);
    task.start();
    return task;
  }


  public static boolean verifyThriftFields(Class<?> clazz){
    TFieldIdEnum timestampField = null;
    TFieldIdEnum seqIdField = null;
    TFieldIdEnum payloadField = null;
    TFieldIdEnum loggingAuditHeadersField = null;
    try {
      Map m = (Map) clazz.getDeclaredField("metaDataMap").get(null);
      for (Object o : m.keySet()) {
        TFieldIdEnum tf = (TFieldIdEnum) o;
        switch(tf.getFieldName()){
          case "timestamp": timestampField = tf; break;
          case "seqId": seqIdField = tf; break;
          case "payload": payloadField = tf; break;
          case "loggingAuditHeaders": loggingAuditHeadersField = tf; break;
          default: throw new Exception("Unexpected field in this class");
        }
      }
      if (timestampField == null || seqIdField == null || payloadField == null || loggingAuditHeadersField == null){
        throw new Exception("Thrift class field is not correctly initialized.");
      }
      LOG.info("fields are {}, {}, {}, {}", timestampField, seqIdField, payloadField, loggingAuditHeadersField);
      return true;
    } catch (Exception e){
      LOG.error("Init error for {}", clazz.getName());
      return false;
    }
  }

  /*
  public static void createThriftLoggers(String loggingAuditClientPath){

    ThriftLoggerFactory.initialize();
    ThriftLoggerFactory.ThriftLoggerFactoryInterface instance = ThriftLoggerFactory.getThriftLoggerFactoryInstance();
    assert instance.getClass().isAssignableFrom(AuditableLogbackThriftLoggerFactory.class);
    assert ((AuditableLogbackThriftLoggerFactory)instance).getLoggingAuditClient() == null;
    ThriftLoggerConfig testLogger2config = new ThriftLoggerConfig(logDir, "test2", 3600, 1024 * 1024);
    ThriftLogger testLogger2 = ThriftLoggerFactory.getLogger(testLogger2config);
    assert testLogger2.getClass().isAssignableFrom(AuditableLogbackThriftLogger.class);
    assert Double.compare(((AuditableLogbackThriftLogger) testLogger2).getAuditSamplingRate(), 1.0) == 0;
    assert ((AuditableLogbackThriftLogger) testLogger2).isEnableLoggingAudit() == false;



    ThriftLoggerConfig auditableLogger1Config = new ThriftLoggerConfig(logDir, "audit_log_1_demo", 3600, 1024 * 1024, null, true);
    ThriftLogger auditableLogger1 = ThriftLoggerFactory.getLogger(auditableLogger1Config);
    assert auditableLogger1.getClass().isAssignableFrom(AuditableLogbackThriftLogger.class);
    assert Double.compare(((AuditableLogbackThriftLogger)  auditableLogger1).getAuditSamplingRate(), 1.0) == 0;
    assert ((AuditableLogbackThriftLogger)  auditableLogger1).isEnableLoggingAudit() == true;


    ThriftLoggerConfig auditableLogger2Config = new ThriftLoggerConfig(logDir, "audit_log_2_demo", 3600, 1024 * 1024, null, true, 0.01);
    ThriftLogger auditableLogger2 = ThriftLoggerFactory.getLogger(auditableLogger2Config);
    assert auditableLogger2.getClass().isAssignableFrom(AuditableLogbackThriftLogger.class);
    assert Double.compare(((AuditableLogbackThriftLogger)  auditableLogger2).getAuditSamplingRate(), 0.01) == 0;
    assert ((AuditableLogbackThriftLogger)  auditableLogger2).isEnableLoggingAudit() == true;
    LOG.info("******************* Logging audit client stored topic configs:  {}", ((AuditableLogbackThriftLoggerFactory) instance).getLoggingAuditClient().getAuditConfigs());

    verifyThriftFields(AuditDemoLog1Message.class);
    verifyThriftFields(AuditDemoLog2Message.class);
    verifyThriftFields(AuditDemoLog3Message.class);

    LoggingTask task1 = new LoggingTask("AuditLog1Demo", auditableLogger1, AuditDemoLog1Message.class, 1,10);
    task1.start();
  }
   */

}
