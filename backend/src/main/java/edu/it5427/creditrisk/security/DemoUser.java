package edu.it5427.creditrisk.security;

import java.util.Set;

public record DemoUser(
        String username,
        String passwordHash,
        Set<Role> roles
) {}
