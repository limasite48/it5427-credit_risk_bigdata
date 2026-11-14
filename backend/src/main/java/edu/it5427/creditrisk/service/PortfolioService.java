package edu.it5427.creditrisk.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

@Service
public class PortfolioService {

    private final AppProperties props;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile DashboardDtos.OverviewResponse cached;

    public PortfolioService(AppProperties props) {
        this.props = props;
    }

    @PostConstruct
    void load() {
        refresh();
    }

    public DashboardDtos.OverviewResponse getOverview() {
        if (cached == null) {
            refresh();
        }
        return cached;
    }

    public synchronized void refresh() {
        try {
            Path summaryPath = Path.of(props.riskSummary()).toAbsolutePath().normalize();
            Path csvPath = Path.of(props.scoredCsv()).toAbsolutePath().normalize();
            JsonNode root = objectMapper.readTree(Files.readString(summaryPath));
            JsonNode portfolio = root.path("portfolio_summary");
            JsonNode financial = root.path("financial_exposure");

            JsonNode delinquency = root.path("delinquency_indicators");
            long totalApplications = portfolio.path("total_applications").asLong();
            long defaultCount = portfolio.path("defaults_count").asLong();
            long nonDefaultCount = portfolio.path("non_defaults_count").asLong();
            double nplRate = portfolio.path("npl_default_ratio").asDouble();
            String nplPercent = portfolio.path("npl_default_percent").asText();
            double averageLoan = financial.path("average_loan_amount").asDouble();
            double medianLoan = financial.path("median_loan_amount").asDouble();
            double averageIncome = financial.path("average_income").asDouble();
            double medianIncome = financial.path("median_income").asDouble();
            double averageAnnuity = financial.path("average_annuity").asDouble();
            double creditToIncome = financial.path("average_credit_to_income_ratio").asDouble();
            double def30 = delinquency.path("DEF_30_CNT_SOCIAL_CIRCLE_positive_rate").asDouble();
            double def60 = delinquency.path("DEF_60_CNT_SOCIAL_CIRCLE_positive_rate").asDouble();

            Double modelAuc = null;
            Double baselineAuc = null;
            Path evalPath = Path.of(props.modelEvaluation()).toAbsolutePath().normalize();
            if (Files.isRegularFile(evalPath)) {
                JsonNode eval = objectMapper.readTree(Files.readString(evalPath));
                modelAuc = eval.path("lightgbm_classifier").path("val_roc_auc").asDouble();
                baselineAuc = eval.path("baseline_logistic_regression").path("val_roc_auc").asDouble();
            }

            TierCounts tiers = countTiers(csvPath);
            cached = new DashboardDtos.OverviewResponse(
                    totalApplications,
                    defaultCount,
                    nonDefaultCount,
                    nplRate,
                    nplPercent,
                    averageLoan,
                    medianLoan,
                    averageIncome,
                    medianIncome,
                    averageAnnuity,
                    creditToIncome,
                    def30,
                    def60,
                    modelAuc,
                    baselineAuc,
                    new DashboardDtos.RiskDistribution(tiers.low, tiers.medium, tiers.high),
                    tiers.total
            );
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Portfolio data unavailable");
        }
    }

    private TierCounts countTiers(Path csvPath) throws IOException {
        long low = 0;
        long medium = 0;
        long high = 0;
        long total = 0;
        try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
            String header = reader.readLine();
            if (header == null) {
                return new TierCounts(0, 0, 0, 0);
            }
            String[] cols = header.split(",", -1);
            int tierIdx = indexOf(cols, "RISK_TIER");
            if (tierIdx < 0) {
                throw new IOException("RISK_TIER column missing");
            }
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = splitCsvLine(line, cols.length);
                if (parts.length <= tierIdx) {
                    continue;
                }
                String tier = parts[tierIdx].trim().toUpperCase();
                switch (tier) {
                    case "LOW" -> low++;
                    case "MEDIUM" -> medium++;
                    case "HIGH" -> high++;
                    default -> {
                    }
                }
                total++;
            }
        }
        return new TierCounts(low, medium, high, total);
    }

    private static int indexOf(String[] cols, String name) {
        for (int i = 0; i < cols.length; i++) {
            if (name.equals(cols[i].trim())) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Minimal CSV splitter that respects quoted fields for Home Credit rows.
     */
    static String[] splitCsvLine(String line, int expected) {
        java.util.List<String> fields = new java.util.ArrayList<>(expected);
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString());
        return fields.toArray(new String[0]);
    }

    public Map<String, Boolean> dataFileChecks() {
        Map<String, Boolean> checks = new HashMap<>();
        checks.put("riskSummary", Files.isRegularFile(Path.of(props.riskSummary())));
        checks.put("scoredCsv", Files.isRegularFile(Path.of(props.scoredCsv())));
        checks.put("demographics", Files.isRegularFile(Path.of(props.demographics())));
        checks.put("rocCurve", Files.isRegularFile(Path.of(props.rocCurve())));
        return checks;
    }

    private record TierCounts(long low, long medium, long high, long total) {}
}
