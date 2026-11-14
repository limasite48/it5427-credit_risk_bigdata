package edu.it5427.creditrisk.controller;

import edu.it5427.creditrisk.dto.PipelineDtos;
import edu.it5427.creditrisk.service.PipelineInsightService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/insights")
public class PipelineInsightController {

    private final PipelineInsightService pipelineInsightService;

    public PipelineInsightController(PipelineInsightService pipelineInsightService) {
        this.pipelineInsightService = pipelineInsightService;
    }

    @GetMapping("/pipeline")
    public PipelineDtos.PipelineResponse pipeline() {
        return pipelineInsightService.getPipeline();
    }
}
