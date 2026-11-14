package edu.it5427.creditrisk.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.it5427.creditrisk.config.AppProperties;
import edu.it5427.creditrisk.dto.DashboardDtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class MlScoringClient {

    private static final Logger log = LoggerFactory.getLogger(MlScoringClient.class);

    private final RestClient restClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String token;

    public MlScoringClient(RestClient.Builder builder, AppProperties props) {
        this.token = props.mlServiceToken();
        if (token == null || token.isBlank()) {
            throw new IllegalStateException("ML_SERVICE_TOKEN must be configured");
        }
        this.restClient = builder.baseUrl(props.mlServiceUrl()).build();
    }

    public boolean isHealthy() {
        try {
            String raw = restClient.get()
                    .uri("/health")
                    .retrieve()
                    .body(String.class);
            if (raw == null) {
                return false;
            }
            JsonNode body = objectMapper.readTree(raw);
            return body.path("model_loaded").asBoolean(false);
        } catch (Exception e) {
            return false;
        }
    }

    public DashboardDtos.ScoreResponse score(Map<String, Object> applicant) {
        try {
            Map<String, Object> envelope = new HashMap<>();
            envelope.put("applicant", applicant);
            String payload = objectMapper.writeValueAsString(envelope);

            String raw = restClient.post()
                    .uri("/score")
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-Internal-Token", token)
                    .body(payload)
                    .retrieve()
                    .body(String.class);

            if (raw == null || raw.isBlank()) {
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Scoring service unavailable");
            }
            return mapScore(objectMapper.readTree(raw));
        } catch (ResponseStatusException e) {
            throw e;
        } catch (RestClientException e) {
            log.warn("ML scoring call failed: {}", e.getClass().getSimpleName());
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Scoring service unavailable");
        } catch (Exception e) {
            log.warn("ML scoring response parse failed: {}", e.getClass().getSimpleName());
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Scoring service unavailable");
        }
    }

    private DashboardDtos.ScoreResponse mapScore(JsonNode node) {
        List<DashboardDtos.RiskDriver> drivers = new ArrayList<>();
        JsonNode driversNode = node.path("top_risk_drivers");
        if (driversNode.isArray()) {
            for (JsonNode d : driversNode) {
                String effect = d.path("effect").asText("");
                boolean raises = effect.toLowerCase().contains("increases");
                Double shap = d.has("shap_value") && !d.get("shap_value").isNull()
                        ? d.get("shap_value").asDouble()
                        : null;
                Object featureValue;
                JsonNode fv = d.get("feature_value");
                if (fv == null || fv.isNull()) {
                    featureValue = null;
                } else if (fv.isNumber()) {
                    featureValue = fv.numberValue();
                } else {
                    featureValue = fv.asText();
                }
                drivers.add(new DashboardDtos.RiskDriver(
                        d.path("feature").asText(),
                        featureValue,
                        shap,
                        effect,
                        raises
                ));
            }
        }

        Object applicationId = null;
        if (node.has("SK_ID_CURR") && !node.get("SK_ID_CURR").isNull()) {
            JsonNode idNode = node.get("SK_ID_CURR");
            if (idNode.isNumber()) {
                applicationId = idNode.numberValue();
            } else {
                applicationId = idNode.asText();
            }
        }
        return new DashboardDtos.ScoreResponse(
                applicationId,
                node.path("default_probability").asDouble(),
                node.path("risk_tier").asText(),
                node.path("action").asText(),
                node.path("tier_description").asText(null),
                drivers
        );
    }
}
