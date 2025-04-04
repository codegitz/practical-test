package io.codegitz.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
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
 * @ClassName EnrichmentService
 * @Description
 * Service for enriching trade data by mapping product IDs to names and validating input formats.
 * <p>
 * Processes CSV data in streaming mode to handle large datasets efficiently. Key features:
 * - Validates date formats (yyyyMMdd with strict resolution)
 * - Maps product IDs to names via {@link ProductService}
 * - Formats prices by removing trailing zeros
 * - Batched output flushing for memory optimization
 * </p>
 * @Author codegitz
 * @Date 2025/4/2
 * @Version v1.0.0
 */
@Component
public class EnrichmentService {
    private static final Logger logger = LoggerFactory.getLogger(EnrichmentService.class);

    /**
     * Thread-safe date formatter with strict resolution.
     * <p>
     * Configures:
     * - Base pattern: yyyyMMdd
     * - Default era to AD (value 1) to handle years without era explicitly
     * - Strict resolver to reject invalid dates like 2023-02-29
     * </p>
     */
    private final DateTimeFormatter dateFormatter = new DateTimeFormatterBuilder()
            .appendPattern("yyyyMMdd")
            .parseDefaulting(ChronoField.ERA, 1)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);

    @Autowired
    private ProductService productService;

    /**
     * Number of records to process before flushing the output buffer.
     * <p>
     * Tuning guidance:
     * - Smaller values: Better memory usage, more frequent I/O
     * - Larger values: Fewer I/O ops, higher memory footprint
     * </p>
     */
    private static final int BATCH_SIZE = 1000;

    /**
     * Creates a streaming processor for trade data enrichment.
     *
     * @param inputStream Input stream of trade CSV data with headers:
     *                    date, productId, currency, price
     * @return StreamingResponseBody that processes data incrementally
     */
    public StreamingResponseBody enrichTrades(InputStream inputStream) {
        return outputStream -> {
            try (InputStreamReader reader = new InputStreamReader(inputStream);
                 CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
                 OutputStreamWriter writer = new OutputStreamWriter(outputStream);
                 CSVPrinter printer = new CSVPrinter(writer,
                         CSVFormat.DEFAULT.builder()
                                 .setHeader("date", "productName", "currency", "price")
                                 .build())) {

                int processedCount = 0;
                for (CSVRecord record : parser) {
                    processTradeRecord(record, printer);

                    // Periodic flush to balance memory and performance
                    if (++processedCount % BATCH_SIZE == 0) {
                        writer.flush();
                        logger.debug("Flushed buffer after {} records", processedCount);
                    }
                }

                // Final flush to ensure data completeness
                writer.flush();
                logger.info("Completed processing {} records", processedCount);
            } catch (Exception e) {
                logger.error("Failed to process CSV stream", e);
                throw e;
            }
        };
    }

    /**
     * Processes a single trade record with validation and enrichment.
     *
     * @param record  CSV record containing raw trade data
     * @param printer CSV printer for writing enriched records
     */
    private void processTradeRecord(CSVRecord record, CSVPrinter printer) throws IOException {
        // 1. Date validation
        final String dateStr = record.get("date");
        if (!isValidDate(dateStr)) {
            return;
        }

        // 2. Product name resolution
        final String productId = record.get("productId");
        final String productName = resolveProductName(productId, record.getRecordNumber());

        // 3. Price formatting
        final String formattedPrice = formatPrice(record.get("price"), record.getRecordNumber());

        // 4. Write enriched record
        printer.printRecord(dateStr, productName, record.get("currency"), formattedPrice);
    }

    /**
     * Validates date string against strict yyyyMMdd format.
     *
     * @return true if valid, false otherwise (logs error automatically)
     */
    private boolean isValidDate(String dateStr) {
        try {
            LocalDate.parse(dateStr, dateFormatter);
            return true;
        } catch (DateTimeParseException e) {
            logger.error("Invalid date format: {} ({})", dateStr, e.getMessage());
            return false;
        }
    }

    /**
     * Resolves product name from ID, handling missing entries.
     */
    private String resolveProductName(String productId, long recordNumber) {
        final String name = productService.getProductName(productId);
        if ("Missing Product Name".equals(name)) {
            logger.warn("Unmapped product ID: {} (record {})", productId, recordNumber);
        }
        return name;
    }

    /**
     * Formats price string by removing trailing zeros.
     * Preserves original value if formatting fails.
     */
    private String formatPrice(String rawPrice, long recordNumber) {
        try {
            return new BigDecimal(rawPrice).stripTrailingZeros().toPlainString();
        } catch (NumberFormatException e) {
            logger.error("Invalid price format: {} (record {})", rawPrice, recordNumber);
            return rawPrice;
        }
    }
}
