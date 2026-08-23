package io.github.idoly.pi.vertx.bedrock;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** AWS Signature Version 4 request signing for Bedrock Runtime. */
public final class AwsSigV4 {
    private static final DateTimeFormatter DATE = DateTimeFormatter
            .ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter
            .ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

    private AwsSigV4() {
    }

    public static Map<String, String> sign(
            URI uri,
            String method,
            byte[] body,
            Map<String, String> headers,
            String region,
            String service,
            AwsCredentials credentials,
            Clock clock
    ) {
        String payloadHash = sha256(body);
        String timestamp = TIMESTAMP.format(clock.instant());
        String date = DATE.format(clock.instant());
        LinkedHashMap<String, String> signed = new LinkedHashMap<>(headers);
        signed.put("host", host(uri));
        signed.put("x-amz-date", timestamp);
        signed.put("x-amz-content-sha256", payloadHash);
        if (credentials.sessionToken() != null
                && !credentials.sessionToken().isBlank()) {
            signed.put("x-amz-security-token", credentials.sessionToken());
        }
        CanonicalHeaders canonical = canonicalHeaders(signed);
        String canonicalRequest = method.toUpperCase(Locale.ROOT) + '\n'
                + canonicalPath(uri) + '\n'
                + (uri.getRawQuery() == null ? "" : uri.getRawQuery()) + '\n'
                + canonical.value() + '\n'
                + canonical.names() + '\n' + payloadHash;
        String scope = date + '/' + region + '/' + service + "/aws4_request";
        String stringToSign = "AWS4-HMAC-SHA256\n" + timestamp + '\n'
                + scope + '\n'
                + sha256(canonicalRequest.getBytes(StandardCharsets.UTF_8));
        byte[] key = hmac(
                hmac(hmac(hmac(
                        ("AWS4" + credentials.secretAccessKey())
                                .getBytes(StandardCharsets.UTF_8), date
                ), region), service), "aws4_request"
        );
        String signature = HexFormat.of().formatHex(hmac(key, stringToSign));
        signed.put("authorization", "AWS4-HMAC-SHA256 Credential="
                + credentials.accessKeyId() + '/' + scope
                + ", SignedHeaders=" + canonical.names()
                + ", Signature=" + signature);
        return Map.copyOf(signed);
    }

    private static CanonicalHeaders canonicalHeaders(Map<String, String> headers) {
        ArrayList<Map.Entry<String, String>> values = new ArrayList<>();
        headers.forEach((name, value) -> values.add(Map.entry(
                name.toLowerCase(Locale.ROOT).strip(),
                value.strip().replaceAll("\\s+", " ")
        )));
        values.sort(Comparator.comparing(Map.Entry::getKey));
        StringBuilder canonical = new StringBuilder();
        StringBuilder names = new StringBuilder();
        for (Map.Entry<String, String> value : values) {
            canonical.append(value.getKey()).append(':')
                    .append(value.getValue()).append('\n');
            if (!names.isEmpty()) names.append(';');
            names.append(value.getKey());
        }
        return new CanonicalHeaders(canonical.toString(), names.toString());
    }

    private static String canonicalPath(URI uri) {
        String path = uri.getRawPath();
        return path == null || path.isEmpty() ? "/" : path;
    }

    private static String host(URI uri) {
        int port = uri.getPort();
        if (port < 0 || uri.getScheme().equals("https") && port == 443
                || uri.getScheme().equals("http") && port == 80) {
            return uri.getHost();
        }
        return uri.getHost() + ':' + port;
    }

    private static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value)
            );
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static byte[] hmac(byte[] key, String value) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private record CanonicalHeaders(String value, String names) {
    }
}
