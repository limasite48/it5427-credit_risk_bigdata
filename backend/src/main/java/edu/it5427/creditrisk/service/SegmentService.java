package edu.it5427.creditrisk.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.it5427.creditrisk.config.AppProperties;
import edu.it5427.creditrisk.dto.DashboardDtos;
import jakarta.annotation.PostConstruct;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Service
public class SegmentService {

    private final AppProperties props;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile DashboardDtos.SegmentsResponse cached;

    public SegmentService(AppProperties props) {
        this.props = props;
    }

    @PostConstruct
    void load() {
        refresh();
    }

    public DashboardDtos.SegmentsResponse getSegments() {
        if (cached == null) {
            refresh();
        }
        return cached;
    }

    public synchronized void refresh() {
        try {
            Path path = Path.of(props.demographics()).toAbsolutePath().normalize();
            JsonNode root = objectMapper.readTree(Files.readString(path));
            cached = new DashboardDtos.SegmentsResponse(
                    parseSection(root.path("risk_by_occupation")),
                    parseSection(root.path("risk_by_income_type")),
                    parseSection(root.path("risk_by_education")),
                    parseSection(root.path("risk_by_ext_source_2_quintile"))
            );
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Segment data unavailable");
        }
    }

    private List<DashboardDtos.SegmentBucket> parseSection(JsonNode section) {
        List<DashboardDtos.SegmentBucket> buckets = new ArrayList<>();
        if (section == null || section.isMissingNode() || !section.isObject()) {
            return buckets;
        }
        Iterator<Map.Entry<String, JsonNode>> fields = section.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            JsonNode node = entry.getValue();
            buckets.add(new DashboardDtos.SegmentBucket(
                    entry.getKey(),
                    node.path("total_count").asLong(),
                    node.path("default_count").asLong(),
                    node.path("default_rate").asDouble(),
                    node.path("default_percent").asText()
            ));
        }
        buckets.sort(Comparator.comparingDouble(DashboardDtos.SegmentBucket::defaultRate).reversed());
        return buckets;
    }
}
