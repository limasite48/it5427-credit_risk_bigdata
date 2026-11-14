package edu.it5427.creditrisk.service;

import edu.it5427.creditrisk.config.AppProperties;
import edu.it5427.creditrisk.dto.AuthDtos;
import edu.it5427.creditrisk.security.DemoUser;
import edu.it5427.creditrisk.security.InMemoryUserStore;
import edu.it5427.creditrisk.security.JwtService;
import edu.it5427.creditrisk.security.RateLimiterService;
import edu.it5427.creditrisk.security.RefreshTokenStore;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseCookie;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class AuthService {

    public static final String REFRESH_COOKIE = "refresh_token";

    private final InMemoryUserStore userStore;
    private final PasswordEncoder passwordEncoder;
    private final JwtService jwtService;
    private final RefreshTokenStore refreshTokenStore;
    private final RateLimiterService rateLimiterService;
    private final AppProperties props;

    public AuthService(
            InMemoryUserStore userStore,
            PasswordEncoder passwordEncoder,
            JwtService jwtService,
            RefreshTokenStore refreshTokenStore,
            RateLimiterService rateLimiterService,
            AppProperties props
    ) {
        this.userStore = userStore;
        this.passwordEncoder = passwordEncoder;
        this.jwtService = jwtService;
        this.refreshTokenStore = refreshTokenStore;
        this.rateLimiterService = rateLimiterService;
        this.props = props;
    }

    public AuthDtos.LoginResponse login(
            AuthDtos.LoginRequest request,
            String clientKey,
            HttpServletResponse response
    ) {
        rateLimiterService.checkLoginAllowed(clientKey);

        Optional<DemoUser> userOpt = userStore.findByUsername(request.username());
        if (userOpt.isEmpty() || !passwordEncoder.matches(request.password(), userOpt.get().passwordHash())) {
            rateLimiterService.recordLoginFailure(clientKey);
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid username or password");
        }

        rateLimiterService.recordLoginSuccess(clientKey);
        DemoUser user = userOpt.get();
        String accessToken = jwtService.createAccessToken(user);
        String refresh = refreshTokenStore.issue(
                user.username(),
                Instant.now().plus(Duration.ofDays(props.jwt().refreshTokenDays()))
        );
        writeRefreshCookie(response, refresh);
        return new AuthDtos.LoginResponse(
                accessToken,
                "Bearer",
                props.jwt().accessTokenMinutes() * 60,
                toUserInfo(user)
        );
    }

    public AuthDtos.LoginResponse refresh(HttpServletRequest request, HttpServletResponse response) {
        String header = request.getHeader("X-Refresh-Request");
        if (header == null || !"1".equals(header)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Missing refresh header");
        }
        String raw = readRefreshCookie(request)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Not authenticated"));

        var storedOpt = refreshTokenStore.consume(raw);
        if (storedOpt.isEmpty()) {
            // Possible reuse of a rotated token — revoke all for safety if we can identify user later.
            clearRefreshCookie(response);
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Not authenticated");
        }

        var stored = storedOpt.get();
        if (stored.revoked()) {
            refreshTokenStore.detectReuseAndRevoke(stored.username());
            clearRefreshCookie(response);
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Not authenticated");
        }

        DemoUser user = userStore.findByUsername(stored.username())
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Not authenticated"));

        String newRefresh = refreshTokenStore.issue(
                user.username(),
                Instant.now().plus(Duration.ofDays(props.jwt().refreshTokenDays()))
        );
        writeRefreshCookie(response, newRefresh);
        String accessToken = jwtService.createAccessToken(user);
        return new AuthDtos.LoginResponse(
                accessToken,
                "Bearer",
                props.jwt().accessTokenMinutes() * 60,
                toUserInfo(user)
        );
    }

    public void logout(HttpServletRequest request, HttpServletResponse response) {
        readRefreshCookie(request).ifPresent(refreshTokenStore::revoke);
        clearRefreshCookie(response);
    }

    public AuthDtos.UserInfo me(String username) {
        DemoUser user = userStore.findByUsername(username)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Not authenticated"));
        return toUserInfo(user);
    }

    private AuthDtos.UserInfo toUserInfo(DemoUser user) {
        return new AuthDtos.UserInfo(
                user.username(),
                user.roles().stream().map(Enum::name).collect(Collectors.toList())
        );
    }

    private void writeRefreshCookie(HttpServletResponse response, String token) {
        ResponseCookie cookie = ResponseCookie.from(REFRESH_COOKIE, token)
                .httpOnly(true)
                .secure(false)
                .path("/api/auth")
                .sameSite("Strict")
                .maxAge(Duration.ofDays(props.jwt().refreshTokenDays()))
                .build();
        response.addHeader(HttpHeaders.SET_COOKIE, cookie.toString());
    }

    private void clearRefreshCookie(HttpServletResponse response) {
        ResponseCookie cookie = ResponseCookie.from(REFRESH_COOKIE, "")
                .httpOnly(true)
                .secure(false)
                .path("/api/auth")
                .sameSite("Strict")
                .maxAge(0)
                .build();
        response.addHeader(HttpHeaders.SET_COOKIE, cookie.toString());
    }

    private Optional<String> readRefreshCookie(HttpServletRequest request) {
        Cookie[] cookies = request.getCookies();
        if (cookies == null) {
            return Optional.empty();
        }
        return Arrays.stream(cookies)
                .filter(c -> REFRESH_COOKIE.equals(c.getName()))
                .map(Cookie::getValue)
                .filter(v -> v != null && !v.isBlank())
                .findFirst();
    }
}
