package edu.it5427.creditrisk.service;

import edu.it5427.creditrisk.client.MlScoringClient;
import edu.it5427.creditrisk.dto.DashboardDtos;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class HealthService {

    private final MlScoringClient mlScoringClient;
    private final PortfolioService portfolioService;

    public HealthService(MlScoringClient mlScoringClient, PortfolioService portfolioService) {
        this.mlScoringClient = mlScoringClient;
        this.portfolioService = portfolioService;
    }

    public DashboardDtos.HealthResponse getHealth() {
        Map<String, Boolean> fileChecks = portfolioService.dataFileChecks();
        boolean dataOk = fileChecks.values().stream().allMatch(Boolean::booleanValue);
        boolean pythonOk = mlScoringClient.isHealthy();

        Map<String, Boolean> checks = new LinkedHashMap<>();
        checks.put("backend", true);
        checks.put("pythonService", pythonOk);
        checks.put("dataFilesAvailable", dataOk);
        checks.put("modelServiceAvailable", pythonOk);
        checks.putAll(fileChecks);

        return new DashboardDtos.HealthResponse(true, pythonOk, dataOk, pythonOk, checks);
    }
}
