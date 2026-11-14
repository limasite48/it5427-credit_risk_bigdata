package edu.it5427.creditrisk.controller;

import edu.it5427.creditrisk.dto.MacroInsightDtos;
import edu.it5427.creditrisk.service.MacroInsightService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/insights")
public class MacroInsightController {

    private final MacroInsightService macroInsightService;

    public MacroInsightController(MacroInsightService macroInsightService) {
        this.macroInsightService = macroInsightService;
    }

    @GetMapping("/regions")
    public MacroInsightDtos.RegionsResponse regions() {
        return macroInsightService.getRegions();
    }

    @PostMapping("/macro-stress")
    public MacroInsightDtos.MacroStressResponse stress(@Valid @RequestBody MacroInsightDtos.MacroStressRequest request) {
        return macroInsightService.stress(request);
    }
}
