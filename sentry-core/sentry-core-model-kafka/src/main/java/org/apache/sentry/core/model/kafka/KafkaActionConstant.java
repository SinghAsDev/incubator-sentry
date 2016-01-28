/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.core.model.kafka;

/**
 * Actions supported by Kafka on its authorizable resources.
 */
public class KafkaActionConstant {

  public static final String ALL = "ALL";
  public static final String READ = "read";
  public static final String WRITE = "write";
  public static final String CREATE = "create";
  public static final String DELETE = "delete";
  public static final String ALTER = "alter";
  public static final String DESCRIBE = "describe";
  public static final String CLUSTER_ACTION = "clusteraction";
}
