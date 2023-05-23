package io.streamnative.pulsar.handlers.amqp;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

public class AmqpSslConfigs {
    public static final String SSL_LINSTENERS = "ssl.listener";
    public static final String SSL_OPTIONS_CACERTFILE = "ssl.options.cacertfile";
    public static final String SSL_OPTIONS_CERTFILE = "ssl.options.certfile";
    public static final String SSL_OPTIONS_KEYFILE = "ssl.options.keyfile";
    public static final String SSL_OPTIONS_PASSWORD = "ssl.options.password";
    public static final String SSL_OPTIONS_VERSION = "ssl.options.versions";
    public static final String SSL_KEYSTORE_TYPE_CONFIG = "ssl.keystore.type";
    public static final String DEFAULT_SSL_KEYSTORE_TYPE = "JKS";
    public static final String SSL_KEYSTORE_LOCATION_CONFIG = "ssl.keystore.location";
    public static final String SSL_KEYSTORE_PASSWORD_CONFIG = "ssl.keystore.password";
    public static final String SSL_KEY_PASSWORD_CONFIG = "ssl.key.password";

    public static final String SSL_TRUSTSTORE_TYPE_CONFIG = "ssl.truststore.type";
    public static final String DEFAULT_SSL_TRUSTSTORE_TYPE = "JKS";

    public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl.truststore.password";
    public static final String SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location";

    public static final String DEFAULT_SSL_ENABLED_PROTOCOLS = "TLSv1.2,TLSv1.1,TLSv1";
    public static final String SSL_PROVIDER_CONFIG = "ssl.provider";
    public static final String SSL_PROTOCOL_CONFIG = "ssl.protocol";
    public static final String DEFAULT_SSL_PROTOCOL = "TLS";
    public static final String SSL_CIPHER_SUITES_CONFIG = "ssl.cipher.suites";
    public static final String SSL_KEYMANAGER_ALGORITHM_CONFIG = "ssl.keymanager.algorithm";
    public static final Object DEFAULT_SSL_KEYMANGER_ALGORITHM = KeyManagerFactory.getDefaultAlgorithm();
    public static final String SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG ="ssl.secure.random.implementation";
    public static final String SSL_TRUSTMANAGER_ALGORITHM_CONFIG = "ssl.trustmanager.algorithm";
    public static final Object DEFAULT_SSL_TRUSTMANAGER_ALGORITHM = TrustManagerFactory.getDefaultAlgorithm();
//    public static final String SSL_ENABLED_PROTOCOLS_CONFIG = "ssl.enabled.protocols";
}
