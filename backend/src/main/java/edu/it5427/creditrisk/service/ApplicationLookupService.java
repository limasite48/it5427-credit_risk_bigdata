package edu.it5427.creditrisk.service;

import edu.it5427.creditrisk.config.AppProperties;
import edu.it5427.creditrisk.dto.DashboardDtos;
import jakarta.annotation.PostConstruct;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
public class ApplicationLookupService {

    private final AppProperties props;
    private final Map<Long, DashboardDtos.ApplicationResponse> index = new HashMap<>();

    public ApplicationLookupService(AppProperties props) {
        this.props = props;
    }

    @PostConstruct
    void load() {
        refresh();
    }

    public synchronized void refresh() {
        index.clear();
        Path csvPath = Path.of(props.scoredCsv()).toAbsolutePath().normalize();
        try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
            String header = reader.readLine();
            if (header == null) {
                return;
            }
            String[] cols = header.split(",", -1);
            Map<String, Integer> colIndex = new HashMap<>();
            for (int i = 0; i < cols.length; i++) {
                colIndex.put(cols[i].trim(), i);
            }
            require(colIndex, "SK_ID_CURR");
            require(colIndex, "PREDICTED_PD");
            require(colIndex, "RISK_TIER");
            require(colIndex, "ACTION_RECOMMENDATION");

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = PortfolioService.splitCsvLine(line, cols.length);
                long id = Long.parseLong(parts[colIndex.get("SK_ID_CURR")].trim());
                double pd = Double.parseDouble(parts[colIndex.get("PREDICTED_PD")].trim());
                String tier = parts[colIndex.get("RISK_TIER")].trim();
                String action = parts[colIndex.get("ACTION_RECOMMENDATION")].trim();
                Double income = optionalDouble(parts, colIndex, "AMT_INCOME_TOTAL");
                Double credit = optionalDouble(parts, colIndex, "AMT_CREDIT");
                String occupation = optionalString(parts, colIndex, "OCCUPATION_TYPE");
                String education = optionalString(parts, colIndex, "NAME_EDUCATION_TYPE");
                index.put(id, new DashboardDtos.ApplicationResponse(
                        id, pd, tier, action, income, credit, occupation, education
                ));
            }
        } catch (IOException | NumberFormatException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Application index unavailable");
        }
    }

    public DashboardDtos.ApplicationResponse findById(String idRaw) {
        long id;
        try {
            id = Long.parseLong(idRaw.trim());
        } catch (NumberFormatException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Application id must be numeric");
        }
        return Optional.ofNullable(index.get(id))
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Application not found"));
    }

    private static void require(Map<String, Integer> cols, String name) {
        if (!cols.containsKey(name)) {
            throw new IllegalStateException("Missing column: " + name);
        }
    }

    private static Double optionalDouble(String[] parts, Map<String, Integer> cols, String name) {
        Integer idx = cols.get(name);
        if (idx == null || idx >= parts.length) {
            return null;
        }
        String raw = parts[idx].trim();
        if (raw.isEmpty()) {
            return null;
        }
        try {
            return Double.parseDouble(raw);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String optionalString(String[] parts, Map<String, Integer> cols, String name) {
        Integer idx = cols.get(name);
        if (idx == null || idx >= parts.length) {
            return null;
        }
        String raw = parts[idx].trim();
        return raw.isEmpty() ? null : raw;
    }
}
