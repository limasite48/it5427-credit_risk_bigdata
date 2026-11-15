package edu.it5427.creditrisk.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public record AppProperties(
        String dataDir,
        String modelPath,
        String scoredCsv,
        String goldScoredCsv,
        String riskSummary,
        String demographics,
        String rocCurve,
        String shapSummary,
        String modelEvaluation,
        String supplementaryDir,
        String sparkReportsDir,
        String mlServiceUrl,
        String mlServiceToken,
        String corsAllowedOrigins,
        Jwt jwt,
        Auth auth,
        DemoUsers demoUsers
) {
    public record Jwt(String secret, long accessTokenMinutes, long refreshTokenDays) {}

    public record Auth(int loginMaxAttempts, int loginLockoutSeconds, int scoreRateLimitPerMinute) {}

    public record DemoUsers(
            String adminUsername,
            String adminPassword,
            String analystUsername,
            String analystPassword,
            String underwriterUsername,
            String underwriterPassword
    ) {}
}
