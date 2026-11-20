/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.mihkels.omb.worker;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * Serves the worker API over HTTPS, HTTP/2 or HTTP/1.1 by ALPN, from a PEM certificate chain and
 * private key, as written by {@code bin/benchmark-tls-cert}, Helm's {@code genSelfSignedCert} or
 * cert-manager.
 */
final class WorkerTls {
    private static final Pattern PEM =
            Pattern.compile("-----BEGIN ([A-Z ]+)-----([A-Za-z0-9+/=\\s]+)-----END \\1-----");
    // DER prefix of AlgorithmIdentifier { rsaEncryption, NULL }
    private static final byte[] RSA_ALGORITHM =
            HexFormat.of().parseHex("300d06092a864886f70d0101010500");

    private static final SecureRandom RANDOM = new SecureRandom();

    private WorkerTls() {}

    /**
     * Builds an HTTPS connector, to pass to {@code config.jetty.addConnector} in place of Javalin's
     * default HTTP connector. The files are read here, so errors surface before the server starts.
     *
     * @param cert the PEM certificate chain
     * @param key the PEM private key, PKCS#8 or PKCS#1 RSA
     * @param host the address to listen on
     * @param port the port to listen on, or 0 for any free port
     * @return the connector factory
     * @throws IOException when a file cannot be read
     * @throws GeneralSecurityException when the certificate or key is invalid
     */
    static BiFunction<Server, HttpConfiguration, Connector> connector(
            Path cert, Path key, String host, int port) throws IOException, GeneralSecurityException {
        byte[] secret = new byte[16];
        RANDOM.nextBytes(secret);
        String password = HexFormat.of().formatHex(secret);
        SslContextFactory.Server ssl = new SslContextFactory.Server();
        ssl.setKeyStore(keyStore(cert, key, password.toCharArray()));
        ssl.setKeyStorePassword(password);
        return (server, http) -> {
            HttpConfiguration https = new HttpConfiguration(http);
            // Callers reach workers by IP or by names the certificate does not list
            https.addCustomizer(new SecureRequestCustomizer(false));
            HttpConnectionFactory http1 = new HttpConnectionFactory(https);
            HTTP2ServerConnectionFactory http2 = new HTTP2ServerConnectionFactory(https);
            // Clients without ALPN, such as older coordinators, get HTTP/1.1
            ALPNServerConnectionFactory alpn =
                    new ALPNServerConnectionFactory(http2.getProtocol(), http1.getProtocol());
            alpn.setDefaultProtocol(http1.getProtocol());
            ServerConnector connector =
                    new ServerConnector(
                            server, new SslConnectionFactory(ssl, alpn.getProtocol()), alpn, http2, http1);
            connector.setHost(host);
            connector.setPort(port);
            return connector;
        };
    }

    static KeyStore keyStore(Path cert, Path key, char[] password)
            throws IOException, GeneralSecurityException {
        List<? extends Certificate> chain =
                List.copyOf(
                        CertificateFactory.getInstance("X.509")
                                .generateCertificates(new ByteArrayInputStream(Files.readAllBytes(cert))));
        if (chain.isEmpty()) {
            throw new GeneralSecurityException("No certificate in " + cert);
        }
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        store.setKeyEntry("worker", privateKey(key), password, chain.toArray(Certificate[]::new));
        return store;
    }

    static PrivateKey privateKey(Path key) throws IOException, GeneralSecurityException {
        Matcher pem = PEM.matcher(Files.readString(key, StandardCharsets.US_ASCII));
        if (!pem.find()) {
            throw new GeneralSecurityException("No PEM private key in " + key);
        }
        byte[] der = Base64.getMimeDecoder().decode(pem.group(2));
        PKCS8EncodedKeySpec spec =
                switch (pem.group(1)) {
                    case "PRIVATE KEY" -> new PKCS8EncodedKeySpec(der);
                    case "RSA PRIVATE KEY" -> new PKCS8EncodedKeySpec(pkcs8FromPkcs1(der));
                    default ->
                            throw new GeneralSecurityException(
                                    "Unsupported key type \""
                                            + pem.group(1)
                                            + "\" in "
                                            + key
                                            + "; convert it with: openssl pkcs8 -topk8 -nocrypt -in KEY -out KEY.pk8");
                };
        for (String algorithm : List.of("RSA", "EC", "Ed25519")) {
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(spec);
            } catch (InvalidKeySpecException _) {
                // Try the next algorithm
            }
        }
        throw new GeneralSecurityException("Unreadable private key in " + key);
    }

    // PrivateKeyInfo { version 0, rsaEncryption, OCTET STRING pkcs1 }
    private static byte[] pkcs8FromPkcs1(byte[] pkcs1) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.writeBytes(new byte[] {0x02, 0x01, 0x00});
        body.writeBytes(RSA_ALGORITHM);
        body.writeBytes(derHeader(0x04, pkcs1.length));
        body.writeBytes(pkcs1);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(derHeader(0x30, body.size()));
        out.writeBytes(body.toByteArray());
        return out.toByteArray();
    }

    private static byte[] derHeader(int tag, int length) {
        if (length < 0x80) {
            return new byte[] {(byte) tag, (byte) length};
        }
        int bytes = length > 0xFFFF ? 3 : length > 0xFF ? 2 : 1;
        byte[] header = new byte[2 + bytes];
        header[0] = (byte) tag;
        header[1] = (byte) (0x80 | bytes);
        for (int i = 0; i < bytes; i++) {
            header[2 + i] = (byte) ((length >> (8 * (bytes - 1 - i))) & 0xFF);
        }
        return header;
    }
}
