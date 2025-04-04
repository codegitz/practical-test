package io.codegitz.controller;

import io.codegitz.service.EnrichmentService;
import io.codegitz.service.ProductService;
import jakarta.websocket.server.PathParam;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;

/**
 * @ClassName EnrichmentController
 * @Description An enrichment controller that replaces the productId in the input CSV with the corresponding productName, and returns the enriched results.
 * @Author codegitz
 * @Date 2025/4/2
 * @Version v1.0.0
 */
@RestController
@RequestMapping("/api/v1")
public class EnrichmentController {

    @Autowired
    private EnrichmentService enrichmentService;

    @PostMapping(value = "/enrich", consumes = "text/csv", produces = "text/csv")
    public StreamingResponseBody enrichTrades(InputStream inputStream) {
        return enrichmentService.enrichTrades(inputStream);
    }

}
