package edu.it5427.creditrisk;

import edu.it5427.creditrisk.service.MacroInsightService;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MacroInsightServiceTest {

    @Test
    void stressPdIncreasesWithUnemploymentShock() {
        double base = 0.10;
        double stressed = MacroInsightService.stressPd(base, 2.0, 0.0);
        assertThat(stressed).isGreaterThan(base);
        assertThat(MacroInsightService.assignTier(stressed)).isEqualTo("MEDIUM");
    }

    @Test
    void assignTierMatchesPolicyCutoffs() {
        assertThat(MacroInsightService.assignTier(0.04)).isEqualTo("LOW");
        assertThat(MacroInsightService.assignTier(0.10)).isEqualTo("MEDIUM");
        assertThat(MacroInsightService.assignTier(0.20)).isEqualTo("HIGH");
    }
}
