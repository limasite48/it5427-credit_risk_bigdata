package edu.it5427.creditrisk.controller;

import edu.it5427.creditrisk.dto.DashboardDtos;
import edu.it5427.creditrisk.service.ApplicationLookupService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/applications")
public class ApplicationController {

    private final ApplicationLookupService lookupService;

    public ApplicationController(ApplicationLookupService lookupService) {
        this.lookupService = lookupService;
    }

    @GetMapping("/{id}")
    public DashboardDtos.ApplicationResponse get(@PathVariable("id") String id) {
        return lookupService.findById(id);
    }
}
