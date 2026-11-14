package edu.it5427.creditrisk.dto;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public final class MacroInsightDtos {
    private MacroInsightDtos() {}

    public record RegionSlice(
            String regionCode,
            int regionRating,
            long applicationCount,
            long highCount,
            long mediumCount,
            long lowCount,
            double highRate,
            double averagePd,
            double averageUnemployment,
            double averageGdpGrowth,
            double averageCpiYoy
    ) {}

    public record RatingSlice(
            int regionRating,
            long applicationCount,
            long highCount,
            double highRate,
            double averagePd,
            double averageUnemployment,
            double averageGdpGrowth
    ) {}

    public record MacroBucket(
            String label,
            double binStart,
            double binEnd,
            long applicationCount,
            long highCount,
            double highRate,
            double averagePd,
            double averageMacroValue
    ) {}

    public record RegionsResponse(
            long scoredSampleSize,
            long matchedWithMacro,
            String joinKey,
            String note,
            List<RatingSlice> byRegionRating,
            List<RegionSlice> topRegions,
            List<MacroBucket> byUnemploymentQuintile,
            List<MacroBucket> byGdpQuintile,
            InsightSummary insights
    ) {}

    public record InsightSummary(
            String highestRiskRating,
            double highestRiskRatingHighRate,
            String highestUnemploymentBucket,
            double highestUnemploymentBucketHighRate,
            double correlationHint
    ) {}

    public record MacroStressRequest(
            @NotNull @DecimalMin("-10.0") @DecimalMax("10.0") Double unemploymentDeltaPp,
            @NotNull @DecimalMin("-20.0") @DecimalMax("20.0") Double cpiShockPct,
            Integer regionRating,
            String regionCode
    ) {}

    public record TierCounts(long low, long medium, long high) {}

    public record MacroStressResponse(
            double unemploymentDeltaPp,
            double cpiShockPct,
            Integer appliedRegionRatingFilter,
            String appliedRegionCodeFilter,
            long applicantsInScope,
            TierCounts baselineTiers,
            TierCounts stressedTiers,
            double baselineHighRate,
            double stressedHighRate,
            double baselineAveragePd,
            double stressedAveragePd,
            double highTierDelta,
            String methodology
    ) {}
}
