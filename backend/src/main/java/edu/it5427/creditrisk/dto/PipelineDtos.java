package edu.it5427.creditrisk.dto;

import java.util.List;

public final class PipelineDtos {
    private PipelineDtos() {}

    public record SourceVolume(
            String table,
            String sourceId,
            String status,
            long rows,
            long sizeBytes,
            double seconds
    ) {}

    public record CleaningRow(
            String table,
            long rowsIn,
            long rowsOut,
            long quarantined,
            long exactDuplicatesRemoved
    ) {}

    public record GoldTable(String name, long rows) {}

    public record StageTiming(String stage, double seconds) {}

    public record LakeLayer(String layer, long filesUploaded, long bytesUploaded, double seconds) {}

    public record QualityCheck(String name, boolean passed) {}

    public record FeatureGroup(String name, int count) {}

    public record StreamSummary(
            String sink,
            long eventsSent,
            double eventsPerSec,
            int microBatches,
            long inputRows,
            long quarantinedRows,
            long duplicatesDropped,
            long lateRowsDropped,
            long eventsWritten,
            long snapshotCustomers
    ) {}

    public record PipelineResponse(
            String datasetLabel,
            String startedAt,
            String finishedAt,
            boolean fixtureRun,
            String note,
            long bronzeRows,
            long bronzeBytes,
            long silverRows,
            long goldCustomers,
            long goldLoans,
            long trainingLabels,
            int featureCount,
            int checksPassed,
            int checksTotal,
            List<StageTiming> stages,
            List<SourceVolume> sources,
            List<CleaningRow> cleaning,
            List<GoldTable> goldTables,
            List<FeatureGroup> featureGroups,
            List<String> syntheticFeatures,
            List<QualityCheck> checks,
            StreamSummary stream,
            List<LakeLayer> lake
    ) {}
}
