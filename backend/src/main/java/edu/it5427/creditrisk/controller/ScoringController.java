package edu.it5427.creditrisk.controller;

import edu.it5427.creditrisk.dto.DashboardDtos;
import edu.it5427.creditrisk.service.ScoringService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/scoring")
public class ScoringController {

    private final ScoringService scoringService;

    public ScoringController(ScoringService scoringService) {
        this.scoringService = scoringService;
    }

    @PostMapping("/score")
    public DashboardDtos.ScoreResponse score(
            @RequestBody Map<String, Object> applicant,
            Authentication authentication,
            HttpServletRequest request
    ) {
        String rateKey = authentication.getName() + "|" + request.getRemoteAddr();
        return scoringService.score(applicant, rateKey);
    }
}
