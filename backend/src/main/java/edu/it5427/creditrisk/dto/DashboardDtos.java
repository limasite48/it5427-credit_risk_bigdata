package edu.it5427.creditrisk.dto;

import java.util.List;
import java.util.Map;

public final class DashboardDtos {
    private DashboardDtos() {}

    public record RiskDistribution(long low, long medium, long high) {}

    public record OverviewResponse(
            long totalApplications,
            long defaultCount,
            long nonDefaultCount,
            double nplRate,
            String nplPercent,
            double averageLoanAmount,
            double medianLoanAmount,
            double averageIncome,
            double medianIncome,
            double averageAnnuity,
            double averageCreditToIncomeRatio,
            double def30SocialCircleRate,
            double def60SocialCircleRate,
            Double modelValAuc,
            Double baselineValAuc,
            RiskDistribution riskDistribution,
            long scoredSampleSize
    ) {}

    public record SegmentBucket(
            String label,
            long totalCount,
            long defaultCount,
            double defaultRate,
            String defaultPercent
    ) {}

    public record SegmentsResponse(
            List<SegmentBucket> byOccupation,
            List<SegmentBucket> byIncomeType,
            List<SegmentBucket> byEducation,
            List<SegmentBucket> byExtSource2
    ) {}

    public record ApplicationResponse(
            long applicationId,
            double predictedPd,
            String riskTier,
            String actionRecommendation,
            Double income,
            Double creditAmount,
            String occupation,
            String education,
            String scoreSource
    ) {}

    public record RiskDriver(
            String feature,
            Object featureValue,
            Double shapValue,
            String effect,
            boolean raisesRisk
    ) {}

    public record ScoreResponse(
            Object applicationId,
            double defaultProbability,
            String riskTier,
            String action,
            String tierDescription,
            List<RiskDriver> topRiskDrivers
    ) {}

    public record HealthResponse(
            boolean backend,
            boolean pythonService,
            boolean dataFilesAvailable,
            boolean modelServiceAvailable,
            Map<String, Boolean> checks
    ) {}
}
