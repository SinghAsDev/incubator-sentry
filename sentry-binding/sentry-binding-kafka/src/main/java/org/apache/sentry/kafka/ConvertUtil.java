/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.kafka;

import java.util.List;

import kafka.security.auth.Resource;

import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.kafka.Server;

import com.google.common.collect.Lists;

public class ConvertUtil {

  public static List<Authorizable> convertResourceToAuthorizable(String hostname,
      final Resource resource) {
    List<Authorizable> authorizables = Lists.newArrayList();
    authorizables.add(new Server(hostname));
    authorizables.add(new Authorizable() {
      @Override
      public String getTypeName() {
        return resource.resourceType().name();
      }

      @Override
      public String getName() {
        return resource.name();
      }
    });
    return authorizables;
  }

}
