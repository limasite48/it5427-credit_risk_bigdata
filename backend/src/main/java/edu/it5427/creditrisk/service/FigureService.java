package edu.it5427.creditrisk.service;

import edu.it5427.creditrisk.config.AppProperties;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.nio.file.Files;
import java.nio.file.Path;

@Service
public class FigureService {

    private final AppProperties props;

    public FigureService(AppProperties props) {
        this.props = props;
    }

    public Resource rocCurve() {
        Path path = Path.of(props.rocCurve()).toAbsolutePath().normalize();
        if (!Files.isRegularFile(path)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "ROC curve not available");
        }
        return new FileSystemResource(path);
    }
}
