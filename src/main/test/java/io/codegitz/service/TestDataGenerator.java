package io.codegitz.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility class for generating test CSV data for products and trades.
 * <p>
 * Features:
 * - Generates product data with sequential IDs
 * - Creates trade data with configurable valid/invalid entries
 * - Optimized for large dataset generation (millions of records)
 * </p>
 *
 * @ClassName TestDataGenerator
 * @Author codegitz
 * @Date 2025/4/2
 * @Version v1.0.0
 */
public class TestDataGenerator {
    // Formatter for date strings (yyyyMMdd format)
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    // Supported currency codes
    private static final String[] CURRENCIES = {"USD", "EUR", "GBP", "JPY"};

    // Shared random number generator
    private static final Random random = new Random();

    /**
     * Generates product CSV data with sequential IDs.
     *
     * @param path    Output file path
     * @param count   Number of products to generate
     * @throws IOException if file operations fail
     */
    public static void generateProductCsv(Path path, int count) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path);
             CSVPrinter printer = new CSVPrinter(writer,
                     CSVFormat.DEFAULT.builder()
                             .setHeader("productId", "productName")
                             .build())) {

            for (int i = 1; i <= count; i++) {
                printer.printRecord(i, "Product " + i);
            }
        }
    }

    /**
     * Generates trade CSV data with configurable error rates.
     * <p>
     * Data generation strategy:
     * - Dates: 95% valid dates between 2020-01-01 and 2023-12-31, 5% invalid
     * - Product IDs: 80% valid (within maxProductId range), 20% invalid
     * - Prices: Random values between 0.00 and 1000.00
     * </p>
     *
     * @param path          Output file path
     * @param count         Number of trades to generate
     * @param maxProductId  Maximum valid product ID (used for generating invalid IDs)
     * @throws IOException if file operations fail
     */
    public static void generateTradeCsv(Path path, int count, int maxProductId) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path);
             CSVPrinter printer = new CSVPrinter(writer,
                     CSVFormat.DEFAULT.builder()
                             .setHeader("date", "productId", "currency", "price")
                             .build())) {

            LocalDate startDate = LocalDate.of(2020, 1, 1);
            LocalDate endDate = LocalDate.of(2023, 12, 31);

            for (int i = 0; i < count; i++) {
                // Generate dates: 5% invalid (20231332), 95% valid random dates
                String date = i % 20 == 0
                        ? "20231332"  // Invalid date (month 13)
                        : randomDate(startDate, endDate).format(DATE_FORMAT);

                // Generate product IDs: 20% beyond maxProductId range
                int productId = random.nextInt((int)(maxProductId * 1.2)) + 1;

                printer.printRecord(
                        date,
                        productId,
                        CURRENCIES[random.nextInt(CURRENCIES.length)],  // Random currency
                        String.format("%.2f", 1000 * random.nextDouble()) // Random price
                );
            }
        }
    }

    /**
     * Generates a random date within specified range.
     *
     * @param start Inclusive start date
     * @param end   Inclusive end date
     * @return Random LocalDate between start and end
     */
    private static LocalDate randomDate(LocalDate start, LocalDate end) {
        long startEpoch = start.toEpochDay();
        long endEpoch = end.toEpochDay();
        return LocalDate.ofEpochDay(
                ThreadLocalRandom.current().nextLong(startEpoch, endEpoch)
        );
    }
}