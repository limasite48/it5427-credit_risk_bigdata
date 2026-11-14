package edu.it5427.creditrisk.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.it5427.creditrisk.config.AppProperties;
import edu.it5427.creditrisk.dto.PipelineDtos;
import jakarta.annotation.PostConstruct;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class PipelineInsightService {

    private final AppProperties props;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile PipelineDtos.PipelineResponse cached;

    public PipelineInsightService(AppProperties props) {
        this.props = props;
    }

    @PostConstruct
    void load() {
        refresh();
    }

    public PipelineDtos.PipelineResponse getPipeline() {
        if (cached == null) {
            refresh();
        }
        return cached;
    }

    public synchronized void refresh() {
        Path dir = Path.of(props.sparkReportsDir()).toAbsolutePath().normalize();
        Path statsPath = dir.resolve("pipeline_stats.json");
        if (!Files.isRegularFile(statsPath)) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Spark pipeline report unavailable");
        }
        try {
            JsonNode stats = objectMapper.readTree(Files.readString(statsPath));
            JsonNode checksRoot = readOptional(dir.resolve("data_quality/gold_checks.json"));
            JsonNode featuresRoot = readOptional(dir.resolve("data_quality/feature_columns.json"));
            cached = build(stats, checksRoot, featuresRoot);
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Spark pipeline report unavailable");
        }
    }

    private JsonNode readOptional(Path path) throws IOException {
        if (!Files.isRegularFile(path)) {
            return null;
        }
        return objectMapper.readTree(Files.readString(path));
    }

    private PipelineDtos.PipelineResponse build(JsonNode stats, JsonNode checksRoot, JsonNode featuresRoot) {
        String datasetLabel = stats.path("dataset_label").asText("Unknown dataset");
        boolean fixtureRun = datasetLabel.toUpperCase().contains("FIXTURE")
                || datasetLabel.toUpperCase().contains("NOT HOME CREDIT");

        List<PipelineDtos.SourceVolume> sources = new ArrayList<>();
        long bronzeRows = 0;
        long bronzeBytes = 0;
        for (JsonNode row : stats.path("ingestion")) {
            long rows = row.path("rows").asLong();
            long bytes = row.path("size_bytes").asLong();
            bronzeRows += rows;
            bronzeBytes += bytes;
            sources.add(new PipelineDtos.SourceVolume(
                    row.path("table").asText(),
                    row.path("source_id").asText(),
                    row.path("status").asText(),
                    rows,
                    bytes,
                    row.path("seconds").asDouble()
            ));
        }

        List<PipelineDtos.CleaningRow> cleaning = new ArrayList<>();
        long silverRows = 0;
        for (JsonNode row : stats.path("silver")) {
            long out = row.path("rows_out").asLong();
            silverRows += out;
            cleaning.add(new PipelineDtos.CleaningRow(
                    row.path("table").asText(),
                    row.path("rows_in").asLong(),
                    out,
                    row.path("rows_quarantined").asLong(),
                    row.path("exact_duplicates_removed").asLong()
            ));
        }

        List<PipelineDtos.GoldTable> goldTables = new ArrayList<>();
        JsonNode goldRows = stats.path("gold").path("rows");
        goldRows.fieldNames().forEachRemaining(name ->
                goldTables.add(new PipelineDtos.GoldTable(name, goldRows.path(name).asLong()))
        );

        List<PipelineDtos.StageTiming> stages = new ArrayList<>();
        JsonNode timings = stats.path("timings_seconds");
        timings.fieldNames().forEachRemaining(name ->
                stages.add(new PipelineDtos.StageTiming(name, timings.path(name).asDouble()))
        );

        List<PipelineDtos.LakeLayer> lake = new ArrayList<>();
        for (JsonNode row : stats.path("minio_sync")) {
            lake.add(new PipelineDtos.LakeLayer(
                    row.path("layer").asText(),
                    row.path("uploaded").asLong(),
                    row.path("bytes_uploaded").asLong(),
                    row.path("seconds").asDouble()
            ));
        }

        List<PipelineDtos.QualityCheck> checks = new ArrayList<>();
        int passed = 0;
        if (checksRoot != null) {
            for (JsonNode row : checksRoot.path("checks")) {
                boolean ok = row.path("passed").asBoolean(false);
                if (ok) {
                    passed++;
                }
                checks.add(new PipelineDtos.QualityCheck(row.path("check").asText(), ok));
            }
        }

        List<String> featureNames = new ArrayList<>();
        List<String> synthetic = new ArrayList<>();
        if (featuresRoot != null) {
            featuresRoot.path("features").forEach(node -> featureNames.add(node.asText()));
            featuresRoot.path("synthetic_features").forEach(node -> synthetic.add(node.asText()));
        }
        int featureCount = stats.path("gold").path("feature_count").asInt(featureNames.size());

        JsonNode producer = stats.path("producer");
        JsonNode stream = stats.path("stream");
        JsonNode snapshot = stats.path("stream_snapshot");
        PipelineDtos.StreamSummary streamSummary = new PipelineDtos.StreamSummary(
                producer.path("sink").asText(stream.path("source").asText("")),
                producer.path("sent").asLong(),
                producer.path("events_per_sec").asDouble(),
                stream.path("micro_batches").asInt(),
                stream.path("input_rows").asLong(),
                stream.path("quarantined_rows").asLong(),
                stream.path("duplicates_dropped").asLong(),
                stream.path("late_rows_dropped").asLong(),
                stream.path("events_written").asLong(),
                snapshot.path("customers").asLong()
        );

        String note = fixtureRun
                ? "These figures come from the Spark test fixture, not the full Home Credit extract. "
                + "The underwriting model still scores the Track A application sample and does not yet consume the gold feature store."
                : "Spark gold profiles are integrated into this view. "
                + "The underwriting model still scores the Track A application sample until it is retrained on the gold feature store.";

        return new PipelineDtos.PipelineResponse(
                datasetLabel,
                stats.path("started_at").asText(null),
                stats.path("finished_at").asText(null),
                fixtureRun,
                note,
                bronzeRows,
                bronzeBytes,
                silverRows,
                goldRows.path("integrated_credit_profile").asLong(),
                goldRows.path("loan_credit_features").asLong(),
                goldRows.path("training_labels").asLong(),
                featureCount,
                passed,
                checks.size(),
                stages,
                sources,
                cleaning,
                goldTables,
                featureGroups(featureNames),
                synthetic,
                checks,
                streamSummary,
                lake
        );
    }

    private List<PipelineDtos.FeatureGroup> featureGroups(List<String> features) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (String name : List.of(
                "Application",
                "Bureau",
                "Previous loans",
                "Installments",
                "POS / cash",
                "Credit card",
                "Traffic (synthetic)",
                "Engineered ratios"
        )) {
            counts.put(name, 0);
        }
        for (String feature : features) {
            String group = groupOf(feature);
            counts.merge(group, 1, Integer::sum);
        }
        List<PipelineDtos.FeatureGroup> groups = new ArrayList<>();
        counts.forEach((name, count) -> {
            if (count > 0) {
                groups.add(new PipelineDtos.FeatureGroup(name, count));
            }
        });
        return groups;
    }

    private String groupOf(String feature) {
        if (feature.startsWith("bureau_")) {
            return "Bureau";
        }
        if (feature.startsWith("previous_") || feature.equals("number_of_previous_applications")) {
            return "Previous loans";
        }
        if (feature.startsWith("pos_")) {
            return "POS / cash";
        }
        if (feature.startsWith("credit_card_") || feature.equals("credit_utilization")) {
            return "Credit card";
        }
        if (feature.startsWith("traffic_")
                || feature.startsWith("major_violation")
                || feature.startsWith("total_traffic")
                || feature.startsWith("accident_")
                || feature.startsWith("synthetic_")
                || feature.equals("has_synthetic_profile")) {
            return "Traffic (synthetic)";
        }
        if (feature.startsWith("installment_")
                || feature.startsWith("late_")
                || feature.startsWith("payment_")
                || feature.startsWith("unpaid_")
                || feature.startsWith("total_installment")
                || feature.startsWith("total_payment")
                || feature.equals("average_days_late")
                || feature.equals("payment_shortfall")) {
            return "Installments";
        }
        if (feature.equals("loan_to_income")
                || feature.equals("annuity_to_income")
                || feature.equals("debt_to_income")
                || feature.equals("credit_to_goods_ratio")
                || feature.equals("age_years")
                || feature.equals("employment_years")
                || feature.equals("total_outstanding_debt")
                || feature.equals("has_previous_overdue")
                || feature.equals("income_per_family_member")) {
            return "Engineered ratios";
        }
        return "Application";
    }
}
