package edu.it5427.creditrisk.controller;

import edu.it5427.creditrisk.dto.AuthDtos;
import edu.it5427.creditrisk.security.InMemoryUserStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private final InMemoryUserStore userStore;

    public AdminController(InMemoryUserStore userStore) {
        this.userStore = userStore;
    }

    @GetMapping("/users")
    public List<AuthDtos.UserInfo> users() {
        return userStore.findAll().stream()
                .map(u -> new AuthDtos.UserInfo(
                        u.username(),
                        u.roles().stream().map(Enum::name).collect(Collectors.toList())
                ))
                .collect(Collectors.toList());
    }
}
