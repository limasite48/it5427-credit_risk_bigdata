package edu.it5427.creditrisk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import edu.it5427.creditrisk.client.MlScoringClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class ApiIntegrationTest {

    private static final HttpServer mlServer;
    private static final AtomicInteger scoreCalls = new AtomicInteger();
    private static volatile boolean mlDown = false;

    static {
        try {
            mlServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            mlServer.createContext("/health", exchange -> {
                byte[] body = mlDown
                        ? "{\"status\":\"unavailable\",\"model_loaded\":false}".getBytes(StandardCharsets.UTF_8)
                        : "{\"status\":\"ok\",\"model_loaded\":true}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(mlDown ? 503 : 200, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            });
            mlServer.createContext("/score", exchange -> {
                scoreCalls.incrementAndGet();
                if (mlDown) {
                    exchange.sendResponseHeaders(503, -1);
                    exchange.close();
                    return;
                }
                String token = exchange.getRequestHeaders().getFirst("X-Internal-Token");
                if (!"test-ml-token".equals(token)) {
                    exchange.sendResponseHeaders(401, -1);
                    exchange.close();
                    return;
                }
                byte[] body = """
                        {
                          "SK_ID_CURR": 1,
                          "default_probability": 0.03,
                          "risk_tier": "LOW",
                          "action": "Auto-Approve",
                          "tier_description": "Low Risk",
                          "top_risk_drivers": [
                            {"feature":"EXT_SOURCE_2","feature_value":0.8,"shap_value":-0.2,"effect":"Lowers Risk","magnitude":0.2}
                          ]
                        }
                        """.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            });
            mlServer.start();
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Autowired
    MockMvc mockMvc;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    MlScoringClient mlScoringClient;

    @AfterAll
    static void stopStubMl() {
        mlServer.stop(0);
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("app.ml-service-url", () -> "http://127.0.0.1:" + mlServer.getAddress().getPort());
        registry.add("app.scored-csv", () -> "../outputs/scored_applications_sample.csv");
        registry.add("app.risk-summary", () -> "../outputs/metrics/risk_summary.json");
        registry.add("app.demographics", () -> "../outputs/metrics/demographic_profiles.json");
        registry.add("app.roc-curve", () -> "../outputs/figures/roc_curve.png");
        registry.add("app.supplementary-dir", () -> "../ingestion-minio-kafka/data/supplementary");
    }

    private String login(String username, String password) throws Exception {
        MvcResult result = mockMvc.perform(post("/api/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Map.of(
                                "username", username,
                                "password", password
                        ))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accessToken").isNotEmpty())
                .andReturn();
        JsonNode body = objectMapper.readTree(result.getResponse().getContentAsString());
        return body.get("accessToken").asText();
    }

    @Test
    void loginFailsWithBadPassword() throws Exception {
        mockMvc.perform(post("/api/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"username":"admin","password":"wrong"}
                                """))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void unauthorizedRejected() throws Exception {
        mockMvc.perform(get("/api/dashboard/overview"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void overviewAndSegmentsFromRealFiles() throws Exception {
        String token = login("analyst", "AnalystDemo!234");
        mockMvc.perform(get("/api/dashboard/overview").header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalApplications").value(4316))
                .andExpect(jsonPath("$.nplRate").value(0.0811))
                .andExpect(jsonPath("$.scoredSampleSize").value(1079))
                .andExpect(jsonPath("$.riskDistribution.high").value(640));

        mockMvc.perform(get("/api/dashboard/segments").header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.byOccupation").isArray())
                .andExpect(jsonPath("$.byIncomeType").isArray())
                .andExpect(jsonPath("$.byEducation").isArray())
                .andExpect(jsonPath("$.byExtSource2").isArray());
    }

    @Test
    void applicationLookupHitAndMiss() throws Exception {
        String token = login("underwriter", "UnderwriterDemo!234");
        mockMvc.perform(get("/api/applications/298157").header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.applicationId").value(298157))
                .andExpect(jsonPath("$.riskTier").value("HIGH"))
                .andExpect(jsonPath("$.scoreSource").value("gold-validation"));

        mockMvc.perform(get("/api/applications/999999999").header("Authorization", "Bearer " + token))
                .andExpect(status().isNotFound());

        mockMvc.perform(get("/api/applications/not-a-number").header("Authorization", "Bearer " + token))
                .andExpect(status().isBadRequest());
    }

    @Test
    void scoringValidationAndSuccess() throws Exception {
        String token = login("underwriter", "UnderwriterDemo!234");
        mockMvc.perform(post("/api/scoring/score")
                        .header("Authorization", "Bearer " + token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest());

        String payload = """
                {
                  "AMT_INCOME_TOTAL": 300000,
                  "AMT_CREDIT": 500000,
                  "AMT_ANNUITY": 25000,
                  "DAYS_BIRTH": -15340,
                  "DAYS_EMPLOYED": -3120,
                  "OCCUPATION_TYPE": "Managers",
                  "NAME_EDUCATION_TYPE": "Higher education",
                  "EXT_SOURCE_1": 0.78,
                  "EXT_SOURCE_2": 0.72,
                  "EXT_SOURCE_3": 0.69
                }
                """;
        mockMvc.perform(post("/api/scoring/score")
                        .header("Authorization", "Bearer " + token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.riskTier").value("LOW"))
                .andExpect(jsonPath("$.action").value("Auto-Approve"))
                .andExpect(jsonPath("$.topRiskDrivers[0].feature").value("EXT_SOURCE_2"));
    }

    @Test
    void adminEndpointDeniedForAnalyst() throws Exception {
        String token = login("analyst", "AnalystDemo!234");
        mockMvc.perform(get("/api/admin/users").header("Authorization", "Bearer " + token))
                .andExpect(status().isForbidden());
    }

    @Test
    void adminCanListUsersAndHealth() throws Exception {
        String token = login("admin", "AdminDemo!234");
        mockMvc.perform(get("/api/admin/users").header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].username").isNotEmpty());

        mockMvc.perform(get("/api/health").header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.backend").value(true));
    }

    @Test
    void actuatorHealthIsPublic() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk());
    }

    @Test
    void pythonDownReturns503() throws Exception {
        mlDown = true;
        try {
            String token = login("underwriter", "UnderwriterDemo!234");
            String payload = """
                    {
                      "AMT_INCOME_TOTAL": 300000,
                      "AMT_CREDIT": 500000,
                      "AMT_ANNUITY": 25000,
                      "DAYS_BIRTH": -15340,
                      "DAYS_EMPLOYED": -3120,
                      "OCCUPATION_TYPE": "Managers",
                      "NAME_EDUCATION_TYPE": "Higher education",
                      "EXT_SOURCE_1": 0.78,
                      "EXT_SOURCE_2": 0.72,
                      "EXT_SOURCE_3": 0.69
                    }
                    """;
            mockMvc.perform(post("/api/scoring/score")
                            .header("Authorization", "Bearer " + token)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(payload))
                    .andExpect(status().isServiceUnavailable());
            assertThat(mlScoringClient.isHealthy()).isFalse();
        } finally {
            mlDown = false;
        }
    }

    @Test
    void regionsAndMacroStress() throws Exception {
        String token = login("analyst", "AnalystDemo!234");
        mockMvc.perform(get("/api/insights/regions").header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.scoredSampleSize").value(1079))
                .andExpect(jsonPath("$.matchedWithMacro").value(1079))
                .andExpect(jsonPath("$.byRegionRating").isArray())
                .andExpect(jsonPath("$.byUnemploymentQuintile").isArray())
                .andExpect(jsonPath("$.insights.highestRiskRating").isNotEmpty());

        mockMvc.perform(post("/api/insights/macro-stress")
                        .header("Authorization", "Bearer " + token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"unemploymentDeltaPp": 2.0, "cpiShockPct": 3.0}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.applicantsInScope").value(1079))
                .andExpect(jsonPath("$.stressedHighRate").isNumber())
                .andExpect(jsonPath("$.methodology").isNotEmpty());
    }

    @Test
    void sparkPipelineReport() throws Exception {
        mockMvc.perform(get("/api/insights/pipeline"))
                .andExpect(status().isUnauthorized());

        String token = login("analyst", "AnalystDemo!234");
        mockMvc.perform(get("/api/insights/pipeline").header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.fixtureRun").value(false))
                .andExpect(jsonPath("$.bronzeRows").value(799487))
                .andExpect(jsonPath("$.goldCustomers").value(5000))
                .andExpect(jsonPath("$.goldLoans").value(23990))
                .andExpect(jsonPath("$.featureCount").value(175))
                .andExpect(jsonPath("$.checksPassed").value(31))
                .andExpect(jsonPath("$.checksTotal").value(31))
                .andExpect(jsonPath("$.sources").isArray())
                .andExpect(jsonPath("$.featureGroups[?(@.name == 'Bureau')].count", hasItem(11)))
                .andExpect(jsonPath("$.note").isNotEmpty());
    }
}
