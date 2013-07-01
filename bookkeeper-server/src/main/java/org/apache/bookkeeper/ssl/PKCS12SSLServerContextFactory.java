/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.ssl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PKCS12SSLServerContextFactory extends SSLServerContextFactory {

    static final Logger LOG = LoggerFactory.getLogger(PKCS12SSLServerContextFactory.class);

    protected static final String PKCS12_KEY_STORE = "pkcs12KeyStore";
    protected static final String PKCS12_KEY_STORE_PASSWORD = "pkcs12KeyStorePassword";

    public static void setConfiguration(Configuration conf, String pkcs12KeyStore, String pkcs12KeyStorePassword) {
        conf.setProperty(PKCS12_KEY_STORE, pkcs12KeyStore);
        conf.setProperty(PKCS12_KEY_STORE_PASSWORD, pkcs12KeyStorePassword);
    }

    private static InputStream getKeyStoreStream(Configuration conf) throws ConfigurationException,
            IOException {
        String keyStore = conf.getString(PKCS12_KEY_STORE);
        if (null == keyStore) {
            throw new ConfigurationException("Key Store is missing.");
        }
        InputStream certStream = PKCS12SSLServerContextFactory.class.getResourceAsStream(keyStore);
        if (null == certStream) {
            certStream = new FileInputStream(keyStore);
        }
        return certStream;
    }

    private static final String getKeyStorePassword(Configuration conf) {
        return conf.getString(PKCS12_KEY_STORE_PASSWORD, "");
    }

    @Override
    public void initialize(Configuration conf) throws IOException {
        try {
            // Load our Java key store.
            KeyStore ks = KeyStore.getInstance("pkcs12");
            InputStream is = getKeyStoreStream(conf);
            try {
                ks.load(is, getKeyStorePassword(conf).toCharArray());
            } finally {
                is.close();
            }

            // Like ssh-agent.
            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, getKeyStorePassword(conf).toCharArray());

            // Create the SSL context.
            ctx = SSLContext.getInstance("TLS");
            ctx.init(kmf.getKeyManagers(), getTrustManagers(), null);
        } catch (Exception ex) {
            LOG.error("Failed to instantiate pkcs12 ssl context : ", ex);
            throw new IOException("Failed to instantiate pkcs12 ssl context : ", ex);
        }
    }

}
