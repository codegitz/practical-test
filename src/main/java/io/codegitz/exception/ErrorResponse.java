package io.codegitz.exception;

public record ErrorResponse(String code, String message, String details) {}
