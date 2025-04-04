package io.codegitz.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName ProductService
 * @Description Service class for managing product data including initialization, replacement.
 *              Thread-safe operations are ensured via ConcurrentHashMap.
 * @Author codegitz
 * @Date 2025/4/2
 * @Version v1.0.0
 */

@Component
public class ProductService {
    private static final Logger logger = LoggerFactory.getLogger(ProductService.class);

    // Thread-safe map to store product data: productId -> productName
    private volatile ConcurrentHashMap<String, String> productMap = new ConcurrentHashMap<>();

    /**
     * Initializes product data from the default 'product.csv' in classpath.
     * Automatically invoked after bean construction via @PostConstruct.
     *
     * @throws IOException if the default 'product.csv' is not found or parsing fails
     */
    @PostConstruct
    public void initProducts() throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("product.csv")) {
            if (is == null) {
                throw new IOException("Default product.csv file not found in classpath");
            }
            loadProductData(is);
        } catch (Exception e) {
            logger.error("Error loading product data", e);
            throw e;
        }
    }

    /**
     * Replaces all existing product data with new data from the provided InputStream.
     *
     * @param inputStream CSV input stream containing product data (productId, productName)
     * @throws IOException if CSV parsing fails
     */
    public void initProducts(InputStream inputStream) throws IOException {
        ConcurrentHashMap<String, String> newMap = new ConcurrentHashMap<>();
        try (InputStream is = inputStream;
             InputStreamReader reader = new InputStreamReader(is);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            for (CSVRecord record : parser) {
                String productId = record.get("productId");
                String productName = record.get("productName");

                // Warn on duplicate productId in the new dataset
                if (newMap.containsKey(productId)) {
                    logger.warn("Duplicate productId detected: {}", productId);
                }
                newMap.put(productId, productName);
            }

            // Atomically replace the entire product map
            productMap = newMap;
            logger.info("Product data replaced successfully. Total entries: {}", newMap.size());
        }
    }

    /**
     * Updates existing product entries or adds new ones from the provided CSV input stream.
     * Performs incremental updates without clearing existing data.
     *
     * @param inputStream CSV input stream containing update data (productId, productName)
     * @throws IOException if CSV parsing fails
     */
    public void updateProducts(InputStream inputStream) throws IOException {
        try (InputStream is = inputStream;
             InputStreamReader reader = new InputStreamReader(is);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            for (CSVRecord record : parser) {
                String productId = record.get("productId");
                String newProductName = record.get("productName");

                // Atomic update: logs add/update operations
                productMap.compute(productId, (key, oldValue) -> {
                    if (oldValue != null) {
                        logger.info("Updated product: ID={}, OldName={} → NewName={}",
                                key, oldValue, newProductName);
                    } else {
                        logger.info("Added new product: ID={}, Name={}", key, newProductName);
                    }
                    return newProductName;
                });
            }
            logger.info("Product update completed. Current entries: {}", productMap.size());
        }
    }

    /**
     * Retrieves the product name for a given product ID.
     *
     * @param productId The product ID to look up
     * @return Corresponding product name, or "Missing Product Name" if not found
     */
    public String getProductName(String productId) {
        return productMap.getOrDefault(productId, "Missing Product Name");
    }

    //----- Helper Methods -----//

    /**
     * Loads product data from an InputStream into the product map.
     *
     * @param inputStream CSV input stream containing product data
     * @throws IOException if CSV parsing fails
     */
    private void loadProductData(InputStream inputStream) throws IOException {
        try (InputStreamReader reader = new InputStreamReader(inputStream);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            for (CSVRecord record : parser) {
                String productId = record.get("productId");
                String productName = record.get("productName");

                // Warn and overwrite if duplicate exists in the default dataset
                if (productMap.containsKey(productId)) {
                    logger.warn("Duplicate in default data: ID={}, Existing={} → New={}",
                            productId, productMap.get(productId), productName);
                }
                productMap.put(productId, productName);
            }
            logger.info("Default product data loaded. Entries: {}", productMap.size());
        }
    }
}
