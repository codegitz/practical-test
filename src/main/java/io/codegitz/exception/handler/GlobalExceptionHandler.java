package io.codegitz.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;

@RestControllerAdvice
public class GlobalExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /**
     * Handles CSV parsing and data processing errors
     */
    @ExceptionHandler(CsvProcessingException.class)
    public ResponseEntity<ErrorResponse> handleCsvProcessingException(
            CsvProcessingException ex, WebRequest request) {

        ErrorResponse response = new ErrorResponse(
                "CSV_PROCESSING_ERROR",
                ex.getMessage(),
                request.getDescription(false)
        );

        logger.error("CSV processing failed: {}", ex.getMessage(), ex);
        return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
    }

    /**
     * Handles invalid data format errors
     */
    @ExceptionHandler(InvalidDataException.class)
    public ResponseEntity<ErrorResponse> handleInvalidDataException(
            InvalidDataException ex, WebRequest request) {

        ErrorResponse response = new ErrorResponse(
                "INVALID_DATA_FORMAT",
                ex.getMessage(),
                request.getDescription(false)
        );

        logger.warn("Invalid data detected: {}", ex.getMessage());
        return new ResponseEntity<>(response, HttpStatus.UNPROCESSABLE_ENTITY);
    }

    /**
     * Fallback handler for other exceptions
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneralException(
            Exception ex, WebRequest request) {

        ErrorResponse response = new ErrorResponse(
                "INTERNAL_SERVER_ERROR",
                "An unexpected error occurred",
                request.getDescription(false)
        );

        logger.error("Unexpected error: {}", ex.getMessage(), ex);
        return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    // Error response DTO
    record ErrorResponse(String code, String message, String details) {}
}