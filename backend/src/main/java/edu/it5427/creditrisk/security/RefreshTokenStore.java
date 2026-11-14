package edu.it5427.creditrisk.security;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class RefreshTokenStore {

    private final SecureRandom secureRandom = new SecureRandom();
    private final Map<String, StoredRefreshToken> tokensByHash = new ConcurrentHashMap<>();
    private final Map<String, String> latestHashByUser = new ConcurrentHashMap<>();

    public String issue(String username, Instant expiresAt) {
        byte[] raw = new byte[32];
        secureRandom.nextBytes(raw);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
        String hash = hash(token);
        tokensByHash.put(hash, new StoredRefreshToken(username, expiresAt, false));
        latestHashByUser.put(username, hash);
        purgeExpired();
        return token;
    }

    public Optional<StoredRefreshToken> consume(String token) {
        String hash = hash(token);
        StoredRefreshToken stored = tokensByHash.get(hash);
        if (stored == null) {
            return Optional.empty();
        }
        if (stored.expiresAt().isBefore(Instant.now())) {
            tokensByHash.remove(hash);
            return Optional.empty();
        }
        if (stored.revoked()) {
            // Caller must treat revoked=true as reuse and revoke the user's sessions.
            return Optional.of(stored);
        }
        // Rotation: mark used token revoked so a second presentation is reuse.
        tokensByHash.put(hash, new StoredRefreshToken(stored.username(), stored.expiresAt(), true));
        return Optional.of(new StoredRefreshToken(stored.username(), stored.expiresAt(), false));
    }

    public void revoke(String token) {
        String hash = hash(token);
        StoredRefreshToken stored = tokensByHash.remove(hash);
        if (stored != null) {
            latestHashByUser.remove(stored.username(), hash);
        }
    }

    public void revokeAllForUser(String username) {
        String latest = latestHashByUser.remove(username);
        if (latest != null) {
            tokensByHash.remove(latest);
        }
        Iterator<Map.Entry<String, StoredRefreshToken>> it = tokensByHash.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, StoredRefreshToken> entry = it.next();
            if (entry.getValue().username().equals(username)) {
                it.remove();
            }
        }
    }

    public void detectReuseAndRevoke(String username) {
        revokeAllForUser(username);
    }

    private void purgeExpired() {
        Instant now = Instant.now();
        tokensByHash.entrySet().removeIf(e -> e.getValue().expiresAt().isBefore(now));
    }

    static String hash(String token) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashed = digest.digest(token.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hashed);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    public record StoredRefreshToken(String username, Instant expiresAt, boolean revoked) {}
}
