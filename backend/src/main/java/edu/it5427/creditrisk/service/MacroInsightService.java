package edu.it5427.creditrisk.service;

import edu.it5427.creditrisk.config.AppProperties;
import edu.it5427.creditrisk.dto.MacroInsightDtos;
import jakarta.annotation.PostConstruct;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class MacroInsightService {

    private static final String METHODOLOGY =
            "Deterministic stress on scored PDs (no retrain): "
                    + "stressedPd = clip(pd * (1 + 0.06*unemploymentDeltaPp + 0.04*cpiShockPct), 0.0001, 0.9999); "
                    + "tiers use existing cutoffs LOW<0.05, MEDIUM<0.15, else HIGH. "
                    + "Macro fields joined from supplementary region tables via REGION_POPULATION_RELATIVE.";

    private final AppProperties props;
    private final List<EnrichedApplicant> applicants = new ArrayList<>();
    private volatile MacroInsightDtos.RegionsResponse cachedRegions;

    public MacroInsightService(AppProperties props) {
        this.props = props;
    }

    @PostConstruct
    void load() {
        refresh();
    }

    public synchronized void refresh() {
        applicants.clear();
        try {
            Map<String, MacroRegion> macroByPop = loadMacroLookup();
            Path csvPath = Path.of(props.scoredCsv()).toAbsolutePath().normalize();
            try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
                String header = reader.readLine();
                if (header == null) {
                    throw new IOException("Empty scored CSV");
                }
                String[] cols = header.split(",", -1);
                Map<String, Integer> idx = indexColumns(cols);
                require(idx, "SK_ID_CURR", "PREDICTED_PD", "RISK_TIER", "REGION_POPULATION_RELATIVE", "REGION_RATING_CLIENT");

                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = PortfolioService.splitCsvLine(line, cols.length);
                    double pop = Double.parseDouble(parts[idx.get("REGION_POPULATION_RELATIVE")].trim());
                    String popKey = formatPopKey(pop);
                    MacroRegion macro = macroByPop.get(popKey);
                    double pd = Double.parseDouble(parts[idx.get("PREDICTED_PD")].trim());
                    String tier = parts[idx.get("RISK_TIER")].trim().toUpperCase(Locale.ROOT);
                    int rating = Integer.parseInt(parts[idx.get("REGION_RATING_CLIENT")].trim());
                    applicants.add(new EnrichedApplicant(
                            Long.parseLong(parts[idx.get("SK_ID_CURR")].trim()),
                            pd,
                            tier,
                            rating,
                            macro == null ? null : macro.regionCode(),
                            macro == null ? Double.NaN : macro.unemployment(),
                            macro == null ? Double.NaN : macro.gdpGrowth(),
                            macro == null ? Double.NaN : macro.cpiYoy()
                    ));
                }
            }
            cachedRegions = buildRegionsResponse();
        } catch (IOException | NumberFormatException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Macro insight data unavailable");
        }
    }

    public MacroInsightDtos.RegionsResponse getRegions() {
        if (cachedRegions == null) {
            refresh();
        }
        return cachedRegions;
    }

    public MacroInsightDtos.MacroStressResponse stress(MacroInsightDtos.MacroStressRequest request) {
        double unempDelta = request.unemploymentDeltaPp();
        double cpiShock = request.cpiShockPct();
        List<EnrichedApplicant> scope = applicants.stream()
                .filter(a -> request.regionRating() == null || a.regionRating() == request.regionRating())
                .filter(a -> request.regionCode() == null
                        || request.regionCode().isBlank()
                        || request.regionCode().equalsIgnoreCase(a.regionCode()))
                .toList();
        if (scope.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "No applicants match the stress filter");
        }

        long baseLow = 0, baseMed = 0, baseHigh = 0;
        long stressLow = 0, stressMed = 0, stressHigh = 0;
        double basePdSum = 0, stressPdSum = 0;
        for (EnrichedApplicant a : scope) {
            basePdSum += a.predictedPd();
            switch (a.riskTier()) {
                case "LOW" -> baseLow++;
                case "MEDIUM" -> baseMed++;
                default -> baseHigh++;
            }
            double stressedPd = stressPd(a.predictedPd(), unempDelta, cpiShock);
            stressPdSum += stressedPd;
            String stressedTier = assignTier(stressedPd);
            switch (stressedTier) {
                case "LOW" -> stressLow++;
                case "MEDIUM" -> stressMed++;
                default -> stressHigh++;
            }
        }
        long n = scope.size();
        return new MacroInsightDtos.MacroStressResponse(
                unempDelta,
                cpiShock,
                request.regionRating(),
                blankToNull(request.regionCode()),
                n,
                new MacroInsightDtos.TierCounts(baseLow, baseMed, baseHigh),
                new MacroInsightDtos.TierCounts(stressLow, stressMed, stressHigh),
                baseHigh / (double) n,
                stressHigh / (double) n,
                basePdSum / n,
                stressPdSum / n,
                stressHigh - baseHigh,
                METHODOLOGY
        );
    }

    public static double stressPd(double pd, double unemploymentDeltaPp, double cpiShockPct) {
        double factor = 1.0 + (0.06 * unemploymentDeltaPp) + (0.04 * cpiShockPct);
        if (factor < 0.1) {
            factor = 0.1;
        }
        double stressed = pd * factor;
        if (stressed < 0.0001) {
            return 0.0001;
        }
        if (stressed > 0.9999) {
            return 0.9999;
        }
        return stressed;
    }

    public static String assignTier(double pd) {
        if (pd < 0.05) {
            return "LOW";
        }
        if (pd < 0.15) {
            return "MEDIUM";
        }
        return "HIGH";
    }

    private MacroInsightDtos.RegionsResponse buildRegionsResponse() {
        long matched = applicants.stream().filter(a -> a.regionCode() != null).count();

        Map<Integer, MutableAgg> byRating = new HashMap<>();
        Map<String, MutableAgg> byRegion = new HashMap<>();
        for (EnrichedApplicant a : applicants) {
            byRating.computeIfAbsent(a.regionRating(), k -> new MutableAgg()).add(a);
            if (a.regionCode() != null) {
                byRegion.computeIfAbsent(a.regionCode(), k -> new MutableAgg()).add(a);
            }
        }

        List<MacroInsightDtos.RatingSlice> ratingSlices = byRating.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> e.getValue().toRatingSlice(e.getKey()))
                .toList();

        List<MacroInsightDtos.RegionSlice> topRegions = byRegion.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue().n, a.getValue().n))
                .limit(15)
                .map(e -> e.getValue().toRegionSlice(e.getKey()))
                .toList();

        List<EnrichedApplicant> withUnemp = applicants.stream()
                .filter(a -> !Double.isNaN(a.unemployment()))
                .sorted(Comparator.comparingDouble(EnrichedApplicant::unemployment))
                .toList();
        List<EnrichedApplicant> withGdp = applicants.stream()
                .filter(a -> !Double.isNaN(a.gdpGrowth()))
                .sorted(Comparator.comparingDouble(EnrichedApplicant::gdpGrowth))
                .toList();

        List<MacroInsightDtos.MacroBucket> unempBuckets = quintileBuckets(withUnemp, true);
        List<MacroInsightDtos.MacroBucket> gdpBuckets = quintileBuckets(withGdp, false);

        MacroInsightDtos.RatingSlice worstRating = ratingSlices.stream()
                .max(Comparator.comparingDouble(MacroInsightDtos.RatingSlice::highRate))
                .orElse(null);
        MacroInsightDtos.MacroBucket worstUnemp = unempBuckets.stream()
                .max(Comparator.comparingDouble(MacroInsightDtos.MacroBucket::highRate))
                .orElse(null);

        double corrHint = 0;
        if (ratingSlices.size() >= 2) {
            MacroInsightDtos.RatingSlice first = ratingSlices.get(0);
            MacroInsightDtos.RatingSlice last = ratingSlices.get(ratingSlices.size() - 1);
            corrHint = last.highRate() - first.highRate();
        }

        MacroInsightDtos.InsightSummary insights = new MacroInsightDtos.InsightSummary(
                worstRating == null ? "n/a" : ("Rating " + worstRating.regionRating()),
                worstRating == null ? 0 : worstRating.highRate(),
                worstUnemp == null ? "n/a" : worstUnemp.label(),
                worstUnemp == null ? 0 : worstUnemp.highRate(),
                corrHint
        );

        return new MacroInsightDtos.RegionsResponse(
                applicants.size(),
                matched,
                "REGION_POPULATION_RELATIVE → region_dim.region_population_relative",
                "Scored CSV macro columns were empty; values are joined live from supplementary regional tables.",
                ratingSlices,
                topRegions,
                unempBuckets,
                gdpBuckets,
                insights
        );
    }

    private List<MacroInsightDtos.MacroBucket> quintileBuckets(List<EnrichedApplicant> sorted, boolean unemployment) {
        List<MacroInsightDtos.MacroBucket> buckets = new ArrayList<>();
        if (sorted.isEmpty()) {
            return buckets;
        }
        int n = sorted.size();
        int parts = Math.min(5, n);
        for (int q = 0; q < parts; q++) {
            int start = (int) Math.floor(q * n / (double) parts);
            int end = (int) Math.floor((q + 1) * n / (double) parts);
            if (end <= start) {
                continue;
            }
            List<EnrichedApplicant> slice = sorted.subList(start, end);
            MutableAgg agg = new MutableAgg();
            for (EnrichedApplicant a : slice) {
                agg.add(a);
            }
            double binStart = unemployment ? slice.get(0).unemployment() : slice.get(0).gdpGrowth();
            double binEnd = unemployment ? slice.get(slice.size() - 1).unemployment() : slice.get(slice.size() - 1).gdpGrowth();
            double avgMacro = unemployment ? agg.unempSum / agg.n : agg.gdpSum / agg.n;
            String label = "Q" + (q + 1);
            buckets.add(new MacroInsightDtos.MacroBucket(
                    label,
                    round4(binStart),
                    round4(binEnd),
                    agg.n,
                    agg.high,
                    agg.high / (double) agg.n,
                    agg.pdSum / agg.n,
                    round4(avgMacro)
            ));
        }
        return buckets;
    }

    private Map<String, MacroRegion> loadMacroLookup() throws IOException {
        Path dir = Path.of(props.supplementaryDir()).toAbsolutePath().normalize();
        Path dimPath = dir.resolve("region_dim.csv");
        Path unempPath = dir.resolve("regional_unemployment_quarterly.csv");
        Path gdpPath = dir.resolve("regional_gdp_growth_quarterly.csv");
        Path cpiPath = dir.resolve("regional_cpi_monthly.csv");
        if (!Files.isRegularFile(dimPath) || !Files.isRegularFile(unempPath)
                || !Files.isRegularFile(gdpPath) || !Files.isRegularFile(cpiPath)) {
            throw new IOException("Supplementary macro files missing under " + dir);
        }

        Map<String, double[]> latestUnemp = latestByRegion(unempPath, "unemployment_rate_pct");
        Map<String, double[]> latestGdp = latestByRegion(gdpPath, "gdp_growth_yoy_pct");
        Map<String, double[]> latestCpi = latestByRegion(cpiPath, "cpi_yoy_pct");

        Map<String, MacroRegion> byPop = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(dimPath)) {
            String header = reader.readLine();
            String[] cols = header.split(",", -1);
            Map<String, Integer> idx = indexColumns(cols);
            require(idx, "region_code", "region_population_relative", "region_rating");
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = PortfolioService.splitCsvLine(line, cols.length);
                String code = parts[idx.get("region_code")].trim();
                double pop = Double.parseDouble(parts[idx.get("region_population_relative")].trim());
                int rating = Integer.parseInt(parts[idx.get("region_rating")].trim());
                double unemp = latestUnemp.containsKey(code) ? latestUnemp.get(code)[0] : Double.NaN;
                double gdp = latestGdp.containsKey(code) ? latestGdp.get(code)[0] : Double.NaN;
                double cpi = latestCpi.containsKey(code) ? latestCpi.get(code)[0] : Double.NaN;
                byPop.put(formatPopKey(pop), new MacroRegion(code, rating, unemp, gdp, cpi));
            }
        }
        return byPop;
    }

    /**
     * Keep the latest observation per region using release_date ordering when present.
     */
    private Map<String, double[]> latestByRegion(Path path, String valueColumn) throws IOException {
        Map<String, Holding> latest = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String header = reader.readLine();
            String[] cols = header.split(",", -1);
            Map<String, Integer> idx = indexColumns(cols);
            require(idx, "region_code", valueColumn);
            Integer dateIdx = idx.get("release_date");
            String line;
            long rowNum = 0;
            while ((line = reader.readLine()) != null) {
                rowNum++;
                String[] parts = PortfolioService.splitCsvLine(line, cols.length);
                String code = parts[idx.get("region_code")].trim();
                double value = Double.parseDouble(parts[idx.get(valueColumn)].trim());
                String date = dateIdx == null ? "" : parts[dateIdx].trim();
                Holding existing = latest.get(code);
                if (existing == null || date.compareTo(existing.date) > 0 || (date.equals(existing.date) && rowNum > existing.rowNum)) {
                    latest.put(code, new Holding(value, date, rowNum));
                }
            }
        }
        Map<String, double[]> out = new HashMap<>();
        for (Map.Entry<String, Holding> e : latest.entrySet()) {
            out.put(e.getKey(), new double[]{e.getValue().value});
        }
        return out;
    }

    private static Map<String, Integer> indexColumns(String[] cols) {
        Map<String, Integer> idx = new HashMap<>();
        for (int i = 0; i < cols.length; i++) {
            idx.put(cols[i].trim(), i);
        }
        return idx;
    }

    private static void require(Map<String, Integer> idx, String... names) throws IOException {
        for (String name : names) {
            if (!idx.containsKey(name)) {
                throw new IOException("Missing column: " + name);
            }
        }
    }

    private static String formatPopKey(double pop) {
        return String.format(Locale.US, "%.6f", pop);
    }

    private static String blankToNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    private static double round4(double v) {
        return Math.round(v * 10000.0) / 10000.0;
    }

    private record MacroRegion(String regionCode, int regionRating, double unemployment, double gdpGrowth, double cpiYoy) {}

    private record EnrichedApplicant(
            long id,
            double predictedPd,
            String riskTier,
            int regionRating,
            String regionCode,
            double unemployment,
            double gdpGrowth,
            double cpiYoy
    ) {}

    private record Holding(double value, String date, long rowNum) {}

    private static final class MutableAgg {
        long n;
        long high;
        long medium;
        long low;
        double pdSum;
        double unempSum;
        double gdpSum;
        double cpiSum;
        int rating;

        void add(EnrichedApplicant a) {
            n++;
            rating = a.regionRating();
            pdSum += a.predictedPd();
            if (!Double.isNaN(a.unemployment())) {
                unempSum += a.unemployment();
            }
            if (!Double.isNaN(a.gdpGrowth())) {
                gdpSum += a.gdpGrowth();
            }
            if (!Double.isNaN(a.cpiYoy())) {
                cpiSum += a.cpiYoy();
            }
            switch (a.riskTier()) {
                case "LOW" -> low++;
                case "MEDIUM" -> medium++;
                default -> high++;
            }
        }

        MacroInsightDtos.RatingSlice toRatingSlice(int regionRating) {
            return new MacroInsightDtos.RatingSlice(
                    regionRating,
                    n,
                    high,
                    high / (double) n,
                    pdSum / n,
                    unempSum / n,
                    gdpSum / n
            );
        }

        MacroInsightDtos.RegionSlice toRegionSlice(String regionCode) {
            return new MacroInsightDtos.RegionSlice(
                    regionCode,
                    rating,
                    n,
                    high,
                    medium,
                    low,
                    high / (double) n,
                    pdSum / n,
                    unempSum / n,
                    gdpSum / n,
                    cpiSum / n
            );
        }
    }
}
