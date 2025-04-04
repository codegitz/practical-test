package io.codegitz.controller;


import io.codegitz.exception.CsvProcessingException;
import io.codegitz.service.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.InputStream;

/**
 * @ClassName ProductController
 * @Description
* REST controller for product data management operations.
* <p>
* Features:
* - Bulk initialization/replacement of product data
* - Incremental updates to existing product entries
* - Stream-based processing for large datasets
* </p>
 * @Author codegitz
 * @Date 2025/4/2
 * @Version v1.0.0
 */
@RestController
@RequestMapping("/api/v1")
public class ProductController {
    private static final Logger logger = LoggerFactory.getLogger(ProductController.class);

    private final ProductService productService;

    @Autowired
    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    /**
     * Initializes or replaces all product data from CSV input stream.
     * <p>
     * CSV format requirements:
     * - Header row: productId, productName
     * - UTF-8 encoding recommended
     * - Supports datasets up to 1M+ records
     * </p>
     *
     * @param inputStream CSV data stream (auto-closed by Spring)
     * @return Operation result with record count
     */
    @PostMapping(value = "/product/init", consumes = "text/csv")
    public ResponseEntity<String> initProducts(InputStream inputStream) {
        try {
            productService.initProducts(inputStream);
            return ResponseEntity.ok("Product data initialized successfully");
        } catch (Exception e) {
            throw new CsvProcessingException("Product initialization failed", e);
        }
    }

    /**
     * Updates existing products or adds new entries from CSV input.
     * <p>
     * Behavior:
     * - Updates existing entries if productId matches
     * - Adds new entries for unrecognized productIds
     * - Processes updates in atomic batches
     * </p>
     *
     * @param inputStream CSV data stream (auto-closed by Spring)
     * @return Operation result with update statistics
     */
    @PostMapping(value = "/product/update", consumes = "text/csv")
    public ResponseEntity<String> updateProducts(InputStream inputStream) {
        try {
            productService.updateProducts(inputStream);
            return ResponseEntity.ok("Product data updated successfully");
        } catch (Exception e) {
            throw new CsvProcessingException("Product update failed", e);
        }
    }
}
