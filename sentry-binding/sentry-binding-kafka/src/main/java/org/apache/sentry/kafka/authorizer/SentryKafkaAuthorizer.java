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
package org.apache.sentry.kafka.authorizer;

import kafka.network.RequestChannel;
import kafka.security.auth.Acl;
import kafka.security.auth.Allow;
import kafka.security.auth.Allow$;
import kafka.security.auth.Authorizer;
import kafka.security.auth.Operation;
import kafka.security.auth.Operation$;
import kafka.security.auth.Resource;
import kafka.security.auth.ResourceType$;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.kafka.KafkaAuthorizable;
import org.apache.sentry.kafka.ConvertUtil;
import org.apache.sentry.kafka.binding.KafkaAuthBinding;
import org.apache.sentry.kafka.binding.KafkaAuthBindingSingleton;
import org.apache.sentry.kafka.conf.KafkaAuthConf;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.provider.db.generic.service.thrift.TAuthorizable;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.Predef;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.immutable.Map;
import scala.collection.immutable.Set;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;


public class SentryKafkaAuthorizer implements Authorizer {
  
  private static Logger LOG =
      LoggerFactory.getLogger(SentryKafkaAuthorizer.class);

  KafkaAuthBinding binding;
  KafkaAuthConf kafkaAuthConf;
  
  String sentry_site = null;
  List<KafkaPrincipal> super_users = null;
  private final String COMPONENT_NAME = "kafka";
  private final String SERVICE_NAME = "kafka";

  public SentryKafkaAuthorizer() {
    
  }
  
  @Override
  public boolean authorize(RequestChannel.Session session, Operation operation,
                           Resource resource) {
    LOG.debug("Authorizing Session: " + session + " for Operation: " + operation + " on Resource: " + resource);
    final KafkaPrincipal user = session.principal();
    if (isSuperUser(user)) {
      LOG.debug("Allowing SuperUser: " + user + " in " + session + " for Operation: " + operation + " on Resource: " + resource);
      return true;
    }
    LOG.debug("User: " + user + " is not a SuperUser");
    return binding.authorize(session, operation, resource);
  }

  private boolean isSuperUser(KafkaPrincipal user) {
    for (KafkaPrincipal superUser : super_users) {
      if (superUser.equals(user)) {
        return true;
      }
    }
    return false;
  }

  public void addRole(final String role) {
    if (roleExists(role)) {
      throw new KafkaException("Can not create an existing role, " + role + ", again.");
    }

    final UserGroupInformation loginUser = getLoginUser();
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        client.createRole(
            loginUser.getShortUserName(), role, COMPONENT_NAME);
        return null;
      }
    });
  }

  public void addRoleToGroups(final String role, final java.util.Set<String> groups) {
    final UserGroupInformation loginUser = getLoginUser();
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        client.addRoleToGroups(
            loginUser.getShortUserName(), role, COMPONENT_NAME, groups);
        return null;
      }
    });
  }

  @Override
  public void addAcls(Set<Acl> acls, final Resource resource) {
    verifyAcls(acls);
    LOG.info("Adding Acl: acl->" + acls + " resource->" + resource);

    final UserGroupInformation loginUser = getLoginUser();
    final Iterator<Acl> iterator = acls.iterator();
    while (iterator.hasNext()) {
      final Acl acl = iterator.next();
      final String role = getRole(acl);
      if (!roleExists(role)) {
        throw new KafkaException("Can not add Acl for non-existent Role: " + role);
      }
      execute(new Command<Void>() {
        @Override
        public Void run(SentryGenericServiceClient client) throws Exception {
          client.grantPrivilege(
              loginUser.getShortUserName(), role, COMPONENT_NAME, toTSentryPrivilege(acl, resource));
          return null;
        }
      });
    }
  }

  @Override
  public boolean removeAcls(Set<Acl> acls, final Resource resource) {
    verifyAcls(acls);
    LOG.info("Removing Acl: acl->" + acls + " resource->" + resource);
    final UserGroupInformation loginUser = getLoginUser();
    final Iterator<Acl> iterator = acls.iterator();
    while (iterator.hasNext()) {
      final Acl acl = iterator.next();
      final String role = getRole(acl);
      execute(new Command<Void>() {
        @Override
        public Void run(SentryGenericServiceClient client) throws Exception {
          client.dropPrivilege(
              loginUser.getShortUserName(), role, toTSentryPrivilege(acl, resource));
          return null;
        }
      });
    }

    return true;
  }

  @Override
  public boolean removeAcls(final Resource resource) {
    LOG.info("Removing Acls for Resource: resource->" + resource);
    final UserGroupInformation loginUser = getLoginUser();
    List<String> roles = getAllRoles();
    final List<TSentryPrivilege> tSentryPrivileges = getAllPrivileges(roles);
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        for (TSentryPrivilege tSentryPrivilege: tSentryPrivileges) {
          if (isPrivilegeForResource(tSentryPrivilege, resource)) {
            client.dropPrivilege(
                loginUser.getShortUserName(), COMPONENT_NAME, tSentryPrivilege);
          }
        }
        return null;
      }
    });

    return true;
  }

  @Override
  public Set<Acl> getAcls(Resource resource) {
    final Option<Set<Acl>> acls = getAcls().get(resource);
    if (acls.nonEmpty())
      return acls.get();
    return new scala.collection.immutable.HashSet<Acl>();
  }

  @Override
  public Map<Resource, Set<Acl>> getAcls(KafkaPrincipal principal) {
    if (principal.getPrincipalType().toLowerCase().equals("group")) {
      List<String> roles = getRolesforGroup(principal.getName());
      return getAclsForRoles(roles);
    } else {
      LOG.info("Did not recognize Principal type: " + principal.getPrincipalType() + ". Returning Acls for all principals.");
      return getAcls();
    }
  }

  @Override
  public Map<Resource, Set<Acl>> getAcls() {
    final List<String> roles = getAllRoles();
    return getAclsForRoles(roles);
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(java.util.Map<String, ?> configs) {
    final Object sentryKafkaSiteUrlConfig = configs.get(KafkaAuthConf.SENTRY_KAFKA_SITE_URL);
    if (sentryKafkaSiteUrlConfig != null) {
      this.sentry_site = sentryKafkaSiteUrlConfig.toString();
    }
    final Object kafkaSuperUsersConfig = configs.get(KafkaAuthConf.KAFKA_SUPER_USERS);
    if (kafkaSuperUsersConfig != null) {
      getSuperUsers(kafkaSuperUsersConfig.toString());
    }
    LOG.info("Configuring Sentry KafkaAuthorizer: " + sentry_site);
    final KafkaAuthBindingSingleton instance = KafkaAuthBindingSingleton.getInstance(sentry_site);
    this.binding = instance.getAuthBinding();
    this.kafkaAuthConf = instance.getKafkaAuthConf();
  }

  private void getSuperUsers(String kafkaSuperUsers) {
    super_users = new ArrayList<>();
    String[] superUsers = kafkaSuperUsers.split(";");
    for (String superUser : superUsers) {
      if (!superUser.isEmpty()) {
        final String trimmedUser = superUser.trim();
        super_users.add(KafkaPrincipal.fromString(trimmedUser));
        LOG.debug("Adding " + trimmedUser + " to list of Kafka SuperUsers.");
      }
    }
  }

  private List<String> getRolesforGroup(final String groupName) {
    final List<String> roles = new ArrayList<>();
    final UserGroupInformation loginUser = getLoginUser();
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        for( TSentryRole tSentryRole: client.listRolesByGroupName(loginUser.getShortUserName(), groupName, COMPONENT_NAME)) {
          roles.add(tSentryRole.getRoleName());
        }
        return null;
      }
    });

    return roles;
  }

  private SentryGenericServiceClient getClient() throws Exception {
    return SentryGenericServiceClientFactory.create(this.kafkaAuthConf);
  }

  public void dropAllRoles() {
    final List<String> roles = getAllRoles();
    final UserGroupInformation loginUser = getLoginUser();
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        for (String role: roles) {
          client.dropRole(loginUser.getShortUserName(), role, COMPONENT_NAME);
        }
        return null;
      }
    });
  }

  /**
   * A Command is a closure used to pass a block of code from individual
   * functions to execute, which centralizes connection error
   * handling. Command is parameterized on the return type of the function.
   */
  private static interface Command<T> {
    T run(SentryGenericServiceClient client) throws Exception;
  }

  private <T> T execute(Command<T> cmd) throws KafkaException {
    SentryGenericServiceClient client = null;
    try {
      client = getClient();
      return cmd.run(client);
    } catch (SentryUserException ex) {
      String msg = "Unable to excute command on sentry server: " + ex.getMessage();
      LOG.error(msg, ex);
      throw new KafkaException(msg, ex);
    } catch (Exception ex) {
      String msg = "Unable to obtain client:" + ex.getMessage();
      LOG.error(msg, ex);
      throw new KafkaException(msg, ex);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  private TSentryPrivilege toTSentryPrivilege(Acl acl, Resource resource) {
    final List<Authorizable> authorizables = ConvertUtil.convertResourceToAuthorizable(acl.host(), resource);
    final List<TAuthorizable> tAuthorizables = new ArrayList<>();
    for (Authorizable authorizable: authorizables) {
      tAuthorizables.add(new TAuthorizable(authorizable.getTypeName(), authorizable.getName()));
    }
    TSentryPrivilege tSentryPrivilege = new TSentryPrivilege(COMPONENT_NAME, SERVICE_NAME, tAuthorizables, acl.operation().name());
    return tSentryPrivilege;
  }

  private String getRole(Acl acl) {
    return acl.principal().getName();
  }

  private UserGroupInformation getLoginUser() {
    UserGroupInformation loginUser = null;
    try {
      loginUser = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      throw new KafkaException("Failed to get login user information.", e);
    }

    return loginUser;
  }

  private void verifyAcls(Set<Acl> acls) {
    final Iterator<Acl> iterator = acls.iterator();
    while (iterator.hasNext()) {
      final Acl acl = iterator.next();
      assert acl.principal().getPrincipalType().toLowerCase().equals("role") : "Only Acls with KafkaPrincipal of type \"role;\" is supported.";
      assert acl.permissionType().name().equals(Allow.name()): "Only Acls with Permission of type \"Allow\" is supported.";
    }
  }

  private boolean isPrivilegeForResource(TSentryPrivilege tSentryPrivilege, Resource resource) {
    final java.util.Iterator<TAuthorizable> authorizablesIterator = tSentryPrivilege.getAuthorizablesIterator();
    while (authorizablesIterator.hasNext()) {
      TAuthorizable tAuthorizable = authorizablesIterator.next();
      if (tAuthorizable.getType().equals(resource.resourceType().name())) {
        return true;
      }
    }
    return false;
  }

  private List<TSentryPrivilege> getAllPrivileges(final List<String> roles) {
    final List<TSentryPrivilege> tSentryPrivileges = new ArrayList<>();
    final UserGroupInformation loginUser = getLoginUser();
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        for (String role: roles) {
          tSentryPrivileges.addAll(client.listPrivilegesByRoleName(
              loginUser.getShortUserName(), role, COMPONENT_NAME, SERVICE_NAME));
        }
        return null;
      }
    });

    return tSentryPrivileges;
  }

  private List<String> getAllRoles() {
    final List<String> roles = new ArrayList<>();
    final UserGroupInformation loginUser = getLoginUser();
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        for( TSentryRole tSentryRole: client.listAllRoles(loginUser.getShortUserName(), COMPONENT_NAME)) {
          roles.add(tSentryRole.getRoleName());
        }
        return null;
      }
    });

    return roles;
  }

  private Map<Resource, Set<Acl>> getAclsForRoles(final List<String> roles) {
    final java.util.Map<Resource, Set<Acl>> resourceAclsMap = new HashMap<Resource, Set<Acl>>();
    final java.util.Map<String, Set<TSentryPrivilege>> rolePrivilegesMap = new HashMap<String, Set<TSentryPrivilege>>();
    final UserGroupInformation loginUser = getLoginUser();
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        for (String role: roles) {
          final java.util.Set<TSentryPrivilege> rolePrivileges = client.listPrivilegesByRoleName(
              loginUser.getShortUserName(), role, COMPONENT_NAME, SERVICE_NAME);
          final Set<TSentryPrivilege> rolePrivilegesScala =
              scala.collection.JavaConverters.asScalaSetConverter(rolePrivileges).asScala().toSet();
          rolePrivilegesMap.put(role, rolePrivilegesScala);
        }
        return null;
      }
    });

    for (String role: rolePrivilegesMap.keySet()) {
      Set<TSentryPrivilege> privileges = rolePrivilegesMap.get(role);
      final Iterator<TSentryPrivilege> iterator = privileges.iterator();
      while (iterator.hasNext()) {
        TSentryPrivilege privilege = iterator.next();
        final List<TAuthorizable> authorizables = privilege.getAuthorizables();
        String host = null;
        String operation = privilege.getAction();
        for (TAuthorizable tAuthorizable: authorizables) {
          if (tAuthorizable.getType().equals(KafkaAuthorizable.AuthorizableType.SERVER.name())) {
            host = tAuthorizable.getName();
          } else {
            Resource resource = new Resource(ResourceType$.MODULE$.fromString(tAuthorizable.getType()), tAuthorizable.getName());
            if (operation.equals("*")) {
              operation = "All";
            }
            Acl acl = new Acl(new KafkaPrincipal("role", role), Allow$.MODULE$, host, Operation$.MODULE$.fromString(operation));
            java.util.Set<Acl> newAclsJava = new HashSet<>();
            newAclsJava.add(acl);
            addExistingAclsForResource(resourceAclsMap, resource, newAclsJava);
            final scala.collection.mutable.Set<Acl> aclScala = JavaConversions.asScalaSet(newAclsJava);
            resourceAclsMap.put(resource, aclScala.<Acl>toSet());
          }
        }
      }
    }

    return scala.collection.JavaConverters.asScalaMapConverter(resourceAclsMap).asScala().toMap(Predef.<Tuple2<Resource, Set<Acl>>>conforms());
  }

  private void addExistingAclsForResource(java.util.Map<Resource, Set<Acl>> resourceAclsMap, Resource resource, java.util.Set<Acl> newAclsJava) {
    final Set<Acl> existingAcls = resourceAclsMap.get(resource);
    if (existingAcls != null) {
      final Iterator<Acl> aclsIter = existingAcls.iterator();
      while (aclsIter.hasNext()) {
        Acl curAcl = aclsIter.next();
        newAclsJava.add(curAcl);
      }
    }
  }

  private boolean roleExists(String role) {
    return getAllRoles().contains(role);
  }
}
