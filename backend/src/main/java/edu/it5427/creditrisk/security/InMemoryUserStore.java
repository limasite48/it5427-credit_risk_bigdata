package edu.it5427.creditrisk.security;

import edu.it5427.creditrisk.config.AppProperties;
import jakarta.annotation.PostConstruct;
import org.springframework.core.env.Environment;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class InMemoryUserStore {

    private final AppProperties props;
    private final PasswordEncoder passwordEncoder;
    private final Environment environment;
    private final Map<String, DemoUser> users = new ConcurrentHashMap<>();

    public InMemoryUserStore(AppProperties props, PasswordEncoder passwordEncoder, Environment environment) {
        this.props = props;
        this.passwordEncoder = passwordEncoder;
        this.environment = environment;
    }

    @PostConstruct
    void init() {
        var demo = props.demoUsers();
        require(demo.adminUsername(), demo.adminPassword(), "DEMO_ADMIN");
        require(demo.analystUsername(), demo.analystPassword(), "DEMO_ANALYST");
        require(demo.underwriterUsername(), demo.underwriterPassword(), "DEMO_UNDERWRITER");

        if (isProdProfile()) {
            refusePlaceholder(demo.adminPassword());
            refusePlaceholder(demo.analystPassword());
            refusePlaceholder(demo.underwriterPassword());
        }

        users.put(demo.adminUsername(), new DemoUser(
                demo.adminUsername(),
                passwordEncoder.encode(demo.adminPassword()),
                Set.of(Role.ADMIN)
        ));
        users.put(demo.analystUsername(), new DemoUser(
                demo.analystUsername(),
                passwordEncoder.encode(demo.analystPassword()),
                Set.of(Role.RISK_ANALYST)
        ));
        users.put(demo.underwriterUsername(), new DemoUser(
                demo.underwriterUsername(),
                passwordEncoder.encode(demo.underwriterPassword()),
                Set.of(Role.UNDERWRITER)
        ));
    }

    public Optional<DemoUser> findByUsername(String username) {
        return Optional.ofNullable(users.get(username));
    }

    public Collection<DemoUser> findAll() {
        return users.values();
    }

    private void require(String username, String password, String label) {
        if (username == null || username.isBlank() || password == null || password.isBlank()) {
            throw new IllegalStateException(label + " credentials must be set via environment variables");
        }
    }

    private void refusePlaceholder(String password) {
        String lower = password.toLowerCase();
        if (lower.contains("change-me") || lower.contains("placeholder") || lower.equals("password")) {
            throw new IllegalStateException("Production profile refuses placeholder demo passwords");
        }
    }

    private boolean isProdProfile() {
        for (String profile : environment.getActiveProfiles()) {
            if ("prod".equalsIgnoreCase(profile)) {
                return true;
            }
        }
        return false;
    }
}
