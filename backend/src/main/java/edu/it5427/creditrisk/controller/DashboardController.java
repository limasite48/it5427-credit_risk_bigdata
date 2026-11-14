package edu.it5427.creditrisk.controller;

import edu.it5427.creditrisk.dto.DashboardDtos;
import edu.it5427.creditrisk.service.FigureService;
import edu.it5427.creditrisk.service.PortfolioService;
import edu.it5427.creditrisk.service.SegmentService;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/dashboard")
public class DashboardController {

    private final PortfolioService portfolioService;
    private final SegmentService segmentService;
    private final FigureService figureService;

    public DashboardController(
            PortfolioService portfolioService,
            SegmentService segmentService,
            FigureService figureService
    ) {
        this.portfolioService = portfolioService;
        this.segmentService = segmentService;
        this.figureService = figureService;
    }

    @GetMapping("/overview")
    public DashboardDtos.OverviewResponse overview() {
        return portfolioService.getOverview();
    }

    @GetMapping("/segments")
    public DashboardDtos.SegmentsResponse segments() {
        return segmentService.getSegments();
    }

    @GetMapping("/figures/roc-curve")
    public ResponseEntity<Resource> rocCurve() {
        Resource resource = figureService.rocCurve();
        return ResponseEntity.ok()
                .contentType(MediaType.IMAGE_PNG)
                .body(resource);
    }
}
