package edu.it5427.creditrisk.security;

import edu.it5427.creditrisk.config.AppProperties;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class RateLimiterService {

    private final AppProperties props;
    private final Map<String, AttemptWindow> loginAttempts = new ConcurrentHashMap<>();
    private final Map<String, Instant> loginLockouts = new ConcurrentHashMap<>();
    private final Map<String, AttemptWindow> scoreAttempts = new ConcurrentHashMap<>();

    public RateLimiterService(AppProperties props) {
        this.props = props;
    }

    public void checkLoginAllowed(String key) {
        Instant lockedUntil = loginLockouts.get(key);
        if (lockedUntil != null && lockedUntil.isAfter(Instant.now())) {
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Too many login attempts");
        }
    }

    public void recordLoginFailure(String key) {
        AttemptWindow window = loginAttempts.compute(key, (k, existing) -> bump(existing));
        if (window.hits() >= props.auth().loginMaxAttempts()) {
            loginLockouts.put(key, Instant.now().plusSeconds(props.auth().loginLockoutSeconds()));
            loginAttempts.remove(key);
        }
    }

    public void recordLoginSuccess(String key) {
        loginAttempts.remove(key);
        loginLockouts.remove(key);
    }

    public void checkScoreAllowed(String key) {
        AttemptWindow window = scoreAttempts.compute(key, (k, existing) -> bump(existing));
        if (window.hits() > props.auth().scoreRateLimitPerMinute()) {
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Scoring rate limit exceeded");
        }
    }

    private AttemptWindow bump(AttemptWindow existing) {
        Instant now = Instant.now();
        if (existing == null || existing.windowStart().isBefore(now.minusSeconds(60))) {
            return new AttemptWindow(now, new AtomicInteger(1));
        }
        existing.counter().incrementAndGet();
        return existing;
    }

    private record AttemptWindow(Instant windowStart, AtomicInteger counter) {
        int hits() {
            return counter.get();
        }
    }
}
