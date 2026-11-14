package edu.it5427.creditrisk.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.util.List;

public final class AuthDtos {
    private AuthDtos() {}

    public record LoginRequest(
            @NotBlank @Size(max = 64) String username,
            @NotBlank @Size(max = 128) String password
    ) {}

    public record LoginResponse(
            String accessToken,
            String tokenType,
            long expiresInSeconds,
            UserInfo user
    ) {}

    public record UserInfo(String username, List<String> roles) {}

    public record MessageResponse(String message) {}
}
