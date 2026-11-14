package edu.it5427.creditrisk.controller;

import edu.it5427.creditrisk.dto.AuthDtos;
import edu.it5427.creditrisk.service.AuthService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private final AuthService authService;

    public AuthController(AuthService authService) {
        this.authService = authService;
    }

    @PostMapping("/login")
    public AuthDtos.LoginResponse login(
            @Valid @RequestBody AuthDtos.LoginRequest request,
            HttpServletRequest httpRequest,
            HttpServletResponse httpResponse
    ) {
        String clientKey = clientKey(httpRequest, request.username());
        return authService.login(request, clientKey, httpResponse);
    }

    @PostMapping("/refresh")
    public AuthDtos.LoginResponse refresh(HttpServletRequest request, HttpServletResponse response) {
        return authService.refresh(request, response);
    }

    @PostMapping("/logout")
    public AuthDtos.MessageResponse logout(HttpServletRequest request, HttpServletResponse response) {
        authService.logout(request, response);
        return new AuthDtos.MessageResponse("Logged out");
    }

    @GetMapping("/me")
    public AuthDtos.UserInfo me(Authentication authentication) {
        return authService.me(authentication.getName());
    }

    private String clientKey(HttpServletRequest request, String username) {
        String ip = request.getRemoteAddr() == null ? "unknown" : request.getRemoteAddr();
        return ip + "|" + username;
    }
}
