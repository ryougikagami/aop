package io.streamnative.pulsar.handlers.amqp.utils.ssl;

/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.streamnative.pulsar.handlers.amqp.AmqpServiceConfiguration;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.net.ssl.SSLEngine;

import io.streamnative.pulsar.handlers.amqp.AmqpSslConfigs;
import lombok.extern.slf4j.Slf4j;

import org.eclipse.jetty.util.ssl. SslContextFactory;

/**
 * Helper class for setting up SSL for amqpChannelInitializer.
 * amqp and Pulsar use different way to store SSL keys, this utils only work for amqp.
 */
@Slf4j
public class SSLUtils {
    // A map between amqp SslConfigs and amqpServiceConfiguration.
    public static final Map<String, String> CONFIG_NAME_MAP = ImmutableMap.<String, String>builder()
            .put(AmqpSslConfigs.SSL_OPTIONS_CACERTFILE, "sslOptionsCacertfile")
            .put(AmqpSslConfigs.SSL_OPTIONS_CERTFILE, "sslOptionsCertfile")
            .put(AmqpSslConfigs.SSL_OPTIONS_KEYFILE, "sslOptionsKeyfile")
            .put(AmqpSslConfigs.SSL_OPTIONS_PASSWORD, "sslOptionsPassword")
            .put(AmqpSslConfigs.SSL_OPTIONS_VERSION, "sslOptionsVersions")
            .build();

    public static SslContextFactory.Server createSslContextFactory(
            AmqpServiceConfiguration amqpServiceConfiguration) {
        Builder<String, Object> sslConfigValues = ImmutableMap.builder();

        CONFIG_NAME_MAP.forEach((key, value) -> {
            Object obj = null;
            switch(key) {
                case AmqpSslConfigs.SSL_OPTIONS_CACERTFILE:
                    obj = amqpServiceConfiguration.getAmqpSslOptionsCacertfile();
                    break;
                case AmqpSslConfigs.SSL_OPTIONS_CERTFILE:
                    obj = amqpServiceConfiguration.getAmqpSslOptionsCertfile();
                    break;
                case AmqpSslConfigs.SSL_OPTIONS_KEYFILE:
                    // this obj is Set<String>
                    obj = amqpServiceConfiguration.getAmqpSslOptionsKeyfile();
                    break;
                case AmqpSslConfigs.SSL_OPTIONS_PASSWORD:
                    obj = amqpServiceConfiguration.getAmqpSslOptionsPassword();
                    break;
                case AmqpSslConfigs.SSL_KEYSTORE_LOCATION_CONFIG:
                    obj = amqpServiceConfiguration.getAmqpSslKeystoreLocation();
                    break;
                case AmqpSslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG:
                    obj = amqpServiceConfiguration.getAmqpSslKeystorePassword();
                    break;
                case AmqpSslConfigs.SSL_KEY_PASSWORD_CONFIG:
                    obj = amqpServiceConfiguration.getAmqpSslKeyPassword();
                    break;
                case AmqpSslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG:
                    obj = amqpServiceConfiguration.getAmqpSslTruststoreLocation();
                    break;
                case AmqpSslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG:
                    obj = amqpServiceConfiguration.getAmqpSslTruststorePassword();
                    break;
                default:
                    log.error("key {} not contained in amqpServiceConfiguration", key);
            }
            if (obj != null) {
                sslConfigValues.put(key, obj);
            }
        });
        return createSslContextFactory(sslConfigValues.build());
    }

    public static SslContextFactory.Server createSslContextFactory(Map<String, Object> sslConfigValues) {
        SslContextFactory.Server ssl = new SslContextFactory.Server();

        configureSslContextFactoryKeyStore(ssl, sslConfigValues);
        configureSslContextFactoryTrustStore(ssl, sslConfigValues);
        configureSslContextFactoryAlgorithms(ssl, sslConfigValues);
        configureSslContextFactoryAuthentication(ssl, sslConfigValues);
        ssl.setEndpointIdentificationAlgorithm(null);
        return ssl;
    }

    /**
     * Configures KeyStore related settings in SslContextFactory.
     */
    protected static void configureSslContextFactoryKeyStore(SslContextFactory ssl,
                                                             Map<String, Object> sslConfigValues) {
        ssl.setKeyStoreType((String)
                getOrDefault(sslConfigValues, AmqpSslConfigs.SSL_KEYSTORE_TYPE_CONFIG, AmqpSslConfigs.DEFAULT_SSL_KEYSTORE_TYPE));

        String sslKeystoreLocation = (String) sslConfigValues.get(AmqpSslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        if (sslKeystoreLocation != null) {
            ssl.setKeyStorePath(sslKeystoreLocation);
        }

//        Password sslKeystorePassword =
//                new Password((String) sslConfigValues.get(AmqpSslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
        if (AmqpSslConfigs.SSL_KEY_PASSWORD_CONFIG != null) {
            ssl.setKeyStorePassword((String) sslConfigValues.get(AmqpSslConfigs.SSL_KEY_PASSWORD_CONFIG));
        }

//        Password sslKeyPassword =
//                new Password((String) sslConfigValues.get(AmqpSslConfigs.SSL_KEY_PASSWORD_CONFIG));
        if (AmqpSslConfigs.SSL_KEY_PASSWORD_CONFIG != null) {
            ssl.setKeyManagerPassword((String) sslConfigValues.get(AmqpSslConfigs.SSL_KEY_PASSWORD_CONFIG));
        }
    }

    protected static Object getOrDefault(Map<String, Object> configMap, String key, Object defaultValue) {
        if (configMap.containsKey(key)) {
            return configMap.get(key);
        }

        return defaultValue;
    }

    /**
     * Configures TrustStore related settings in SslContextFactory.
     */
    protected static void configureSslContextFactoryTrustStore(SslContextFactory ssl,
                                                               Map<String, Object> sslConfigValues) {
        ssl.setTrustStoreType(
                (String) getOrDefault(
                        sslConfigValues,
                        AmqpSslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
                        AmqpSslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE));

        String sslTruststoreLocation = (String) sslConfigValues.get(AmqpSslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        if (sslTruststoreLocation != null) {
            ssl.setTrustStorePath(sslTruststoreLocation);
        }

//        Password sslTruststorePassword =
//                new Password((String) sslConfigValues.get(SSL_TRUSTSTORE_PASSWORD_CONFIG));
//        if (sslTruststorePassword != null) {
//            ssl.setTrustStorePassword(sslTruststorePassword.value());
//        }
        if (sslConfigValues.get(AmqpSslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG) != null) {
            ssl.setTrustStorePassword((String) sslConfigValues.get(AmqpSslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        }
    }

    /**
     * Configures Protocol, Algorithm and Provider related settings in SslContextFactory.
     */
    protected static void configureSslContextFactoryAlgorithms(SslContextFactory ssl,
                                                               Map<String, Object> sslConfigValues) {
        Set<String> sslEnabledProtocols =
                (Set<String>) getOrDefault(
                        sslConfigValues,
                        AmqpSslConfigs.SSL_OPTIONS_VERSION,
                        Arrays.asList(AmqpSslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS.split("\\s*,\\s*")).stream().collect(Collectors.toSet()));
        ssl.setIncludeProtocols(sslEnabledProtocols.toArray(new String[sslEnabledProtocols.size()]));

        String sslProvider = (String) sslConfigValues.get(AmqpSslConfigs.SSL_PROVIDER_CONFIG);
        if (sslProvider != null) {
            ssl.setProvider(sslProvider);
        }

        ssl.setProtocol(
                (String) getOrDefault(sslConfigValues, AmqpSslConfigs.SSL_PROTOCOL_CONFIG, AmqpSslConfigs.DEFAULT_SSL_PROTOCOL));

        Set<String> sslCipherSuites = (Set<String>) sslConfigValues.get(AmqpSslConfigs.SSL_CIPHER_SUITES_CONFIG);
        if (sslCipherSuites != null) {
            ssl.setIncludeCipherSuites(sslCipherSuites.toArray(new String[sslCipherSuites.size()]));
        }

        ssl.setKeyManagerFactoryAlgorithm((String) getOrDefault(
                sslConfigValues,
                AmqpSslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
                AmqpSslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM));

        String sslSecureRandomImpl = (String) sslConfigValues.get(AmqpSslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
        if (sslSecureRandomImpl != null) {
            ssl.setSecureRandomAlgorithm(sslSecureRandomImpl);
        }

        ssl.setTrustManagerFactoryAlgorithm((String) getOrDefault(
                sslConfigValues,
                AmqpSslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
                AmqpSslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM));
    }

    /**
     * Configures Authentication related settings in SslContextFactory.
     */
    protected static void configureSslContextFactoryAuthentication(SslContextFactory.Server ssl,
                                                                   Map<String, Object> sslConfigValues) {
//        String sslClientAuth = (String) getOrDefault(
//                sslConfigValues,
//                BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG,
//                "none");
//        switch (sslClientAuth) {
//            case "requested":
//                ssl.setWantClientAuth(true);
//                break;
//            case "required":
//                ssl.setNeedClientAuth(true);
//                break;
//            default:
//                ssl.setNeedClientAuth(false);
//                ssl.setWantClientAuth(false);
//        }
    }

    /**
     * Create SSL engine used in amqpChannelInitializer.
     */
    public static SSLEngine createSslEngine(SslContextFactory.Server sslContextFactory) throws Exception {
        sslContextFactory.start();
        SSLEngine engine  = sslContextFactory.newSSLEngine();
        engine.setUseClientMode(false);

        return engine;
    }

}
