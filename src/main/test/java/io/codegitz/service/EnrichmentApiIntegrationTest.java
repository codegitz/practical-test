package io.codegitz.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.util.StreamUtils;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @ClassName EnrichmentApiIntegrationTest
 * @Description
 * @Author codegitz
 * @Date 2025/4/2
 * @Version v1.0.0
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class EnrichmentApiIntegrationTest {
    private static final Path PRODUCT_CSV = Paths.get("target/test-data/products.csv");
    private static final Path TRADE_CSV = Paths.get("target/test-data/trades.csv");
    private static final Path EXPECTED_CSV = Paths.get("target/test-data/expected.csv");
    private static final Path OUTPUT_CSV = Paths.get("target/test-data/output.csv");

    private static final TestRestTemplate restTemplate = new TestRestTemplate();
    private static final int PRODUCT_COUNT = 100_000;  // 100k product
    private static final int TRADE_COUNT = 1_000_000_0;  // 1000k trade

    @BeforeAll
    static void setup() throws Exception {
        Files.createDirectories(PRODUCT_CSV.getParent());

        // generate test data
        TestDataGenerator.generateProductCsv(PRODUCT_CSV, PRODUCT_COUNT);
        TestDataGenerator.generateTradeCsv(TRADE_CSV, TRADE_COUNT, PRODUCT_COUNT);

        // generate expected result
        generateExpectedResult();
    }
    @Test
    void testEnrichLargeFile() throws Exception {
        // 1. init product data
        initProductData();

        // 2. enrich data
        ResponseEntity<byte[]> response = callEnrichApi();

        // 3. validate result
        saveAndValidateResult(response);
    }

    private void initProductData() throws Exception {
        // initial request
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "text/csv");

        FileSystemResource productFile = new FileSystemResource(PRODUCT_CSV.toFile());
        HttpEntity<FileSystemResource> entity = new HttpEntity<>(productFile, headers);

        // send init product request
        ResponseEntity<String> initResponse = restTemplate.exchange(
                "http://localhost:8080/api/v1/product/init",
                HttpMethod.POST,
                entity,
                String.class
        );

        // assert init result
        assertEquals(HttpStatus.OK, initResponse.getStatusCode());
        assertTrue(initResponse.getBody().contains("successfully"));
    }


    private ResponseEntity<byte[]> callEnrichApi() {
        // use stream to transport file
        InputStreamResource inputStreamResource = null;
        try {
            inputStreamResource = new InputStreamResource(
                    Files.newInputStream(TRADE_CSV)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "text/csv");

        RequestEntity<InputStreamResource> request = RequestEntity
                .post(URI.create("http://localhost:8080/api/v1/enrich"))
                .headers(headers)
                .body(inputStreamResource);

        return restTemplate.exchange(request, byte[].class);
    }

    private void saveAndValidateResult(ResponseEntity<byte[]> response) throws Exception {
        // save output stream to output.csv
        try (OutputStream os = Files.newOutputStream(OUTPUT_CSV)) {
            os.write(response.getBody());
        }

        // generate expected result
        generateExpectedResult();

        // validate results
        validateResults();
    }

    private static void generateExpectedResult() throws Exception {
        try (BufferedReader tradeReader = Files.newBufferedReader(TRADE_CSV);
             BufferedReader productReader = Files.newBufferedReader(PRODUCT_CSV);
             BufferedWriter writer = Files.newBufferedWriter(EXPECTED_CSV)) {

            Map<String , String> products = loadProducts(productReader);

            CSVParser tradeParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(tradeReader);
            CSVPrinter resultPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.builder().setHeader(
                    "date", "productName", "currency", "price"
            ).build());

            for (CSVRecord trade : tradeParser) {
                if (!isValidDate(trade.get("date"))) {
                    continue;
                }

                String  productId = trade.get("productId");
                String productName = products.getOrDefault(productId, "Missing Product Name");

                String price = new BigDecimal(trade.get("price"))
                        .stripTrailingZeros().toPlainString();

                resultPrinter.printRecord(
                        trade.get("date"),
                        productName,
                        trade.get("currency"),
                        price
                );
            }
        }
    }

    private static Map<String, String> loadProducts(BufferedReader reader) throws Exception {
        Map<String, String> map = new HashMap<>();
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
        for (CSVRecord record : parser) {
            map.put(record.get("productId"), record.get("productName"));
        }
        return map;
    }

    private static boolean isValidDate(String dateStr) {
        try {
            LocalDate parse = LocalDate.parse(dateStr, DateTimeFormatter.BASIC_ISO_DATE);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void validateResults() throws IOException {
        try (BufferedReader expectedReader = Files.newBufferedReader(EXPECTED_CSV);
             BufferedReader actualReader = Files.newBufferedReader(OUTPUT_CSV)) {

            CSVParser expectedParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(expectedReader);
            CSVParser actualParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(actualReader);

            Iterator<CSVRecord> expectedIter = expectedParser.iterator();
            Iterator<CSVRecord> actualIter = actualParser.iterator();

            int line = 1;
            while (expectedIter.hasNext() && actualIter.hasNext()) {
                CSVRecord expected = expectedIter.next();
                CSVRecord actual = actualIter.next();

                assertEquals(expected.get("date"), actual.get("date"), "Line " + line + " date mismatch");
                assertEquals(expected.get("productName"), actual.get("productName"), "Line " + line + " productName mismatch");
                assertEquals(expected.get("currency"), actual.get("currency"), "Line " + line + " currency mismatch");
                assertEquals(expected.get("price"), actual.get("price"), "Line " + line + " price mismatch");

                line++;
            }

            assertFalse(expectedIter.hasNext(), "Expected more records");
            assertFalse(actualIter.hasNext(), "Unexpected extra records");
        }
    }
}
