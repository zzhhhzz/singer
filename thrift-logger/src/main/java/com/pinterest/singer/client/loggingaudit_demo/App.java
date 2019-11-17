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

package com.pinterest.singer.client.loggingaudit_demo;

import com.pinterest.singer.client.ThriftLogger;
import com.pinterest.singer.client.ThriftLoggerConfig;
import com.pinterest.singer.client.ThriftLoggerFactory;
import com.pinterest.singer.loggingaudit.client.utils.CommonUtils;
import com.pinterest.singer.loggingaudit.thrift.AuditDemoLog1Message;
import com.pinterest.singer.loggingaudit.thrift.AuditDemoLog2Message;
import com.pinterest.singer.loggingaudit.thrift.AuditDemoLog3Message;
import com.pinterest.singer.metrics.OpenTsdbStatsPusher;
import com.pinterest.singer.metrics.StatsPusher;
import com.pinterest.singer.metrics.OstrichAdminService;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class App {

  private static final String METRICS_PREFIX = "audit_demo";
  private static final Logger LOG = LoggerFactory.getLogger(App.class);
  private static final int STATS_PUSH_INTERVAL_IN_MILLISECONDS = 10 * 1000;
  private static  StatsPusher statsPusher = null;
  private static ThriftLoggerFactory thriftLoggerFactory = null;


  static class CleanupThread extends Thread {

    @Override
    public void run() {

      LOG.info("start to gracefully shutdown thriftLoggerFactory");
      try{
        if (thriftLoggerFactory != null && thriftLoggerFactory.getThriftLoggerFactoryInstance() != null){
          thriftLoggerFactory.getThriftLoggerFactoryInstance().shutdown();
        }
      } catch (Throwable t){
        LOG.error("Shutdown thriftLoggerFactory failure :", t);
      }

      LOG.info("start to gracefully shutdown statsPusher");
      try {
        if (statsPusher!= null) {
          statsPusher.sendMetrics(false);
        } else {
          LOG.error("metricsPusher was not initialized properly.");
        }
      } catch (Throwable t) {
        LOG.error("Shutdown failure: metrics : ", t);
      }


    }
  }

  public static void startOstrichService() {
    // do not start ostrich if Ostrich server is disabled
    OstrichAdminService ostrichService = new OstrichAdminService(9001);
    ostrichService.start();

    // enable high granularity metrics we are running in canary

    LOG.info("Starting the stats pusher");
    try {
      statsPusher = new OpenTsdbStatsPusher();
      HostAndPort pushHostPort = HostAndPort.fromString("localhost:18126");
      statsPusher.configure(CommonUtils.getHostName(), METRICS_PREFIX, pushHostPort.getHostText(), pushHostPort.getPort(), STATS_PUSH_INTERVAL_IN_MILLISECONDS);
      statsPusher.start();
      LOG.info("Stats pusher started!");
    } catch (Throwable t) {
      LOG.error("Exception when starting stats pusher: ", t);
    }
  }

  public static void main(String[] args) {
    LOG.warn("Starting thrift logger factory.");
   try {
      Runtime.getRuntime().addShutdownHook(new CleanupThread());
      String configFilePath = System.getProperty("auditclient.config.file");
      LOG.info("config file for auditable logback thrift logger factory: {}" + configFilePath);
      startApp(configFilePath);
      startOstrichService();
    } catch (Throwable t) {
      LOG.error("demo app failed.", t);
      System.exit(1);
    }
  }

  public static void startApp(String configFilePath){
    Preconditions.checkArgument(AppUtils.verifyThriftFields(AuditDemoLog1Message.class));
    Preconditions.checkArgument(AppUtils.verifyThriftFields(AuditDemoLog2Message.class));

    AppUtils.initThriftLoggerFactory(configFilePath);

    ThriftLoggerConfig config1 = AppUtils.createThriftLoggerConfig("audit_log1_demo", 3600 * 2, 1024 * 1024 * 1024, AuditDemoLog1Message.class, true, 0.1);
    ThriftLoggerConfig config2 = AppUtils.createThriftLoggerConfig("audit_log2_demo", 3600 * 2, 1024 * 1024 * 1024, AuditDemoLog2Message.class, true, 1);

    ThriftLogger logger1 = AppUtils.createThriftLogger(config1);
    ThriftLogger logger2 = AppUtils.createThriftLogger(config2);

    AppUtils.createAndStartLoggingTask("AuditLog1Demo", logger1, AuditDemoLog1Message.class, 1000, 100 );
    AppUtils.createAndStartLoggingTask("AuditLog2Demo", logger2, AuditDemoLog2Message.class, 1000_0, 100 );
  }
}