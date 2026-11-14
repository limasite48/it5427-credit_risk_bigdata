package edu.it5427.creditrisk.service;

import edu.it5427.creditrisk.client.MlScoringClient;
import edu.it5427.creditrisk.dto.DashboardDtos;
import edu.it5427.creditrisk.security.RateLimiterService;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@Service
public class ScoringService {

    private static final List<String> REQUIRED_FIELDS = List.of(
            "AMT_INCOME_TOTAL",
            "AMT_CREDIT",
            "AMT_ANNUITY",
            "DAYS_BIRTH",
            "DAYS_EMPLOYED",
            "OCCUPATION_TYPE",
            "NAME_EDUCATION_TYPE",
            "EXT_SOURCE_1",
            "EXT_SOURCE_2",
            "EXT_SOURCE_3"
    );

    private final MlScoringClient mlScoringClient;
    private final RateLimiterService rateLimiterService;

    public ScoringService(MlScoringClient mlScoringClient, RateLimiterService rateLimiterService) {
        this.mlScoringClient = mlScoringClient;
        this.rateLimiterService = rateLimiterService;
    }

    public DashboardDtos.ScoreResponse score(Map<String, Object> applicant, String rateKey) {
        if (applicant == null || applicant.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Applicant payload is required");
        }
        for (String field : REQUIRED_FIELDS) {
            if (!applicant.containsKey(field) || applicant.get(field) == null) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Missing required field: " + field);
            }
        }
        rateLimiterService.checkScoreAllowed(rateKey);
        return mlScoringClient.score(applicant);
    }
}
