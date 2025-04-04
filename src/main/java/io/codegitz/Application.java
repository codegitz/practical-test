package io.codegitz;

import io.codegitz.service.ProductService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * @ClassName Application
 * @Description
 * @Author codegitz
 * @Date 2025/4/2
 * @Version v1.0.0
 */
@SpringBootApplication
public class Application {

    public static void main(String... args) {
        SpringApplication.run(Application.class, args);
    }

    public @Bean ProductService productService() {
        return new ProductService();
    }
}
