/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;
import java.io.File;
import java.net.InetAddress;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;


/**
 * A Kerberos (GSSAPI) implementation of an {@link Authenticator}
 * This authenticator authenticates and autorizes all clients with a valid service ticket.
 *
 * @author Marc de Lignie
 */
public class Krb5Authenticator implements Authenticator {
    private static final Logger logger = LoggerFactory.getLogger(Krb5Authenticator.class);
    private Subject subject;
    private String principalName;

    @Override
    public boolean requireAuthentication() {
        return true;
    }

    /**
     * Called once on server startup with the authentication.config from the *.yaml file
     */
    @Override
    public void setup(final Map<String, Object> config) {
        final String KEYTAB_KEY = "keytab";
        final String PRINCIPAL_KEY = "principal";

        logger.info("Config: {}", config);

        if (null == config || !config.containsKey(KEYTAB_KEY) || !config.containsKey(PRINCIPAL_KEY)) {
            throw new IllegalArgumentException(String.format(
                "Could not configure a %s - provide a 'config' in the 'authentication' settings",
                Krb5Authenticator.class.getName()));
        }

        try {
            final File keytabFile = new File((String) config.get(KEYTAB_KEY));
            principalName = (String) config.get(PRINCIPAL_KEY);
            subject = JaasKrbUtil.loginUsingKeytab(principalName, keytabFile);
        } catch (Exception e) {
            logger.warn("Failed to login to kdc:" + e.getMessage());
        }
        
        logger.debug("Done logging in to kdc");
    }

    @Override
    public SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress) {
        logger.debug("newSaslNegotiator() called");
        return Subject.doAs(subject, (PrivilegedAction<SaslNegotiator>) Krb5SaslAuthenticator::new);
    }

    public AuthenticatedUser authenticate(final Map<String, String> credentials) throws AuthenticationException {
        logger.error("Authenticate() should not be called. Use getAuthenticatedUser() when isComplete() is true.");
        return null;
    }

    /*
     * Krb5SaslAuthenticator is invoked by:
     *   org.apache.tinkerpop.gremlin.server.handler.SaslAuthenticationHandler
     * Negotiates with evaluateChallenge() of the generic sasl handler in gremlin console:
     *   org.apache.tinkerpop.gremlin.driver.Handler.java
     * See also:
     *   https://docs.oracle.com/javase/8/docs/technotes/guides/security/sasl/sasl-refguide.html
     *   https://docs.oracle.com/javase/8/docs/api/org/ietf/jgss/GSSContext.html
     * Kerberos authentication/authorization message flow:
     *   http://www.roguelynn.com/words/explain-like-im-5-kerberos/
     */
    private class Krb5SaslAuthenticator implements Authenticator.SaslNegotiator, CallbackHandler {
        private final String mechanism = "GSSAPI";
        private SaslServer saslServer;

        Krb5SaslAuthenticator() {
            try {
                // For sasl properties regarding GSSAPI, see:
                //   https://docs.oracle.com/javase/8/docs/technotes/guides/security/sasl/sasl-refguide.html#SERVER
                // Rely on GSSAPI defaults for Sasl.MAX_BUFFER and Sasl.QOP. Note, however, that gremlin-driver has
                // Sasl.SERVER_AUTH fixed to true (mutual authentication) and one can configure SSL for enhanced confidentiality,
                // Sasl policy properties for negotiating the authentication mechanism are not relevant here, because
                // GSSAPI is the only available mechanism for this authenticator
                final Map<String, Object> props = new HashMap<>();
                if (principalName == null) {
                    throw new IllegalArgumentException("Principal name cannot be empty. Use principal name of format 'service/fqdn@kdcrealm'");
                }
                final String[] principalParts = principalName.split("/|@");
                if (principalParts.length < 3) throw new IllegalArgumentException("Use principal name of format 'service/fqdn@kdcrealm'");
                saslServer = Sasl.createSaslServer(mechanism, principalParts[0], principalParts[1], props, Krb5SaslAuthenticator.this);
            } catch(Exception e) {
                logger.error("Creating sasl server failed: ", e);
            }
            logger.debug("SaslServer created with: " + saslServer.getMechanismName());
        }

        /*
         * This method is based on the handle() method from:
         *   https://github.com/apache/directory-kerby/blob/kerby-all-1.0.0-RC2/kerby-kerb/integration-test/src/main/java/org/apache/kerby/kerberos/kerb/integration/test/sasl/SaslAppServer.java
         *
         * This provides the simplest form of authorization, where each client that provides a valid service ticket,
         * is authorized. More elaborate authorization schemes would interrogate a policy server like Apache Ranger
         * or ACL's of a storage backend.
         */
        @Override
        public void handle(final Callback[] callbacks) throws UnsupportedCallbackException {
            logger.debug("Krb5 AuthorizeCallback number: " + callbacks.length);
            AuthorizeCallback ac = null;
            for (Callback callback : callbacks) {
                if (callback instanceof AuthorizeCallback) {
                    ac = (AuthorizeCallback) callback;
                } else {
                    throw new UnsupportedCallbackException(callback, "Unrecognized SASL GSSAPI Callback");
                }
            }
            if (ac != null) {
                final String authid = ac.getAuthenticationID();
                final String authzid = ac.getAuthorizationID();
                if (authid.equals(authzid)) {   // Check whether the service ticket belongs to the authenticated user
                    ac.setAuthorized(true);
                } else {
                    ac.setAuthorized(false);
                }
                if (ac.isAuthorized()) {        // Get user name from his principal name
                    final String[] authidParts = authid.split("@");
                    ac.setAuthorizedID(authidParts[0]);
                }
            }
        }

        @Override
        public byte[] evaluateResponse(final byte[] clientResponse) throws AuthenticationException {
            logger.debug("evaluateResponse() length: " + clientResponse.length);
            byte[] response;
            try {
                response = saslServer.evaluateResponse(clientResponse);
            } catch (Exception e) {
                logger.warn("Sasl krb5 evaluateResponse failed: " + e);
                throw new AuthenticationException(e);
            }
            return response;
        }

        @Override
        public boolean isComplete() {
            return saslServer.isComplete();
        }

        @Override
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
            logger.debug("getAuthenticatedUser called: " + saslServer.getAuthorizationID());
            return new AuthenticatedUser(saslServer.getAuthorizationID());
        }

    }
}