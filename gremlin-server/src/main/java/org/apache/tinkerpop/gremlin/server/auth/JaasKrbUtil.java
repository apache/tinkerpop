/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License. 
 *
 */
package org.apache.tinkerpop.gremlin.server.auth;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * JAAS utilities for Kerberos login.
 *
 * Except for the package name and this comment, this file is a literal copy from
 * package org.apache.kerby.kerberos.kerb.client, see:
 *     https://github.com/apache/directory-kerby/blob/kerby-all-1.0.0-RC2/kerby-kerb/kerb-simplekdc/
 *         src/main/java/org/apache/kerby/kerberos/kerb/client/JaasKrbUtil.java
 */
public final class JaasKrbUtil {

    public static final boolean ENABLE_DEBUG = false;

    private JaasKrbUtil() { }

    public static Subject loginUsingPassword(
            String principal, String password) throws LoginException {
        Set<Principal> principals = new HashSet<Principal>();
        principals.add(new KerberosPrincipal(principal));

        Subject subject = new Subject(false, principals,
                new HashSet<Object>(), new HashSet<Object>());

        Configuration conf = usePassword(principal);
        String confName = "PasswordConf";
        CallbackHandler callback = new KrbCallbackHandler(principal, password);
        LoginContext loginContext = new LoginContext(confName, subject, callback, conf);
        loginContext.login();
        return loginContext.getSubject();
    }

    public static Subject loginUsingTicketCache(
            String principal, File cacheFile) throws LoginException {
        Set<Principal> principals = new HashSet<Principal>();
        principals.add(new KerberosPrincipal(principal));

        Subject subject = new Subject(false, principals,
                new HashSet<Object>(), new HashSet<Object>());

        Configuration conf = useTicketCache(principal, cacheFile);
        String confName = "TicketCacheConf";
        LoginContext loginContext = new LoginContext(confName, subject, null, conf);
        loginContext.login();
        return loginContext.getSubject();
    }

    public static Subject loginUsingKeytab(
            String principal, File keytabFile) throws LoginException {
        Set<Principal> principals = new HashSet<Principal>();
        principals.add(new KerberosPrincipal(principal));

        Subject subject = new Subject(false, principals,
                new HashSet<Object>(), new HashSet<Object>());

        Configuration conf = useKeytab(principal, keytabFile);
        String confName = "KeytabConf";
        LoginContext loginContext = new LoginContext(confName, subject, null, conf);
        loginContext.login();
        return loginContext.getSubject();
    }

    public static Configuration usePassword(String principal) {
        return new PasswordJaasConf(principal);
    }

    public static Configuration useTicketCache(String principal,
                                               File credentialFile) {
        return new TicketCacheJaasConf(principal, credentialFile);
    }

    public static Configuration useKeytab(String principal, File keytabFile) {
        return new KeytabJaasConf(principal, keytabFile);
    }

    private static String getKrb5LoginModuleName() {
        return System.getProperty("java.vendor").contains("IBM")
                ? "com.ibm.security.auth.module.Krb5LoginModule"
                : "com.sun.security.auth.module.Krb5LoginModule";
    }

    static class KeytabJaasConf extends Configuration {
        private String principal;
        private File keytabFile;

        KeytabJaasConf(String principal, File keytab) {
            this.principal = principal;
            this.keytabFile = keytab;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            Map<String, String> options = new HashMap<String, String>();
            options.put("keyTab", keytabFile.getAbsolutePath());
            options.put("principal", principal);
            options.put("useKeyTab", "true");
            options.put("storeKey", "true");
            options.put("doNotPrompt", "true");
            options.put("renewTGT", "false");
            options.put("refreshKrb5Config", "true");
            options.put("isInitiator", "true");
            options.put("debug", String.valueOf(ENABLE_DEBUG));

            return new AppConfigurationEntry[]{
                    new AppConfigurationEntry(getKrb5LoginModuleName(),
                            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                            options)};
        }
    }

    static class TicketCacheJaasConf extends Configuration {
        private String principal;
        private File clientCredentialFile;

        TicketCacheJaasConf(String principal, File clientCredentialFile) {
            this.principal = principal;
            this.clientCredentialFile = clientCredentialFile;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            Map<String, String> options = new HashMap<String, String>();
            options.put("principal", principal);
            options.put("storeKey", "false");
            options.put("doNotPrompt", "false");
            options.put("useTicketCache", "true");
            options.put("renewTGT", "true");
            options.put("refreshKrb5Config", "true");
            options.put("isInitiator", "true");
            options.put("ticketCache", clientCredentialFile.getAbsolutePath());
            options.put("debug", String.valueOf(ENABLE_DEBUG));

            return new AppConfigurationEntry[]{
                    new AppConfigurationEntry(getKrb5LoginModuleName(),
                            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                            options)};
        }
    }

    static class PasswordJaasConf extends Configuration {
        private String principal;

        PasswordJaasConf(String principal) {
            this.principal = principal;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            Map<String, String> options = new HashMap<>();
            options.put("principal", principal);
            options.put("storeKey", "true");
            options.put("useTicketCache", "true");
            options.put("useKeyTab", "false");
            options.put("renewTGT", "true");
            options.put("refreshKrb5Config", "true");
            options.put("isInitiator", "true");
            options.put("debug", String.valueOf(ENABLE_DEBUG));

            return new AppConfigurationEntry[]{
                    new AppConfigurationEntry(getKrb5LoginModuleName(),
                            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                            options)};
        }
    }

    public static class KrbCallbackHandler implements CallbackHandler {
        private String principal;
        private String password;

        public KrbCallbackHandler(String principal, String password) {
            this.principal = principal;
            this.password = password;
        }

        public void handle(Callback[] callbacks)
                throws IOException, UnsupportedCallbackException {
            for (int i = 0; i < callbacks.length; i++) {
                if (callbacks[i] instanceof PasswordCallback) {
                    PasswordCallback pc = (PasswordCallback) callbacks[i];
                    if (pc.getPrompt().contains(principal)) {
                        pc.setPassword(password.toCharArray());
                        break;
                    }
                }
            }
        }
    }

}
