package pl.mevo.dashboard.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.sql.DataSource;
import com.clickhouse.jdbc.ClickHouseDataSource;

import java.sql.SQLException;
import java.util.Properties;

@Configuration
@EnableConfigurationProperties({ClickHouseProperties.class, AppProperties.class})
public class WebConfig {

    @Bean
    DataSource clickHouseDataSource(ClickHouseProperties props) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", props.username());
        properties.setProperty("password", props.password());
        return new ClickHouseDataSource(props.url(), properties);
    }

    @Bean
    WebMvcConfigurer corsConfigurer(AppProperties appProperties) {
        String[] origins = appProperties.cors().allowedOrigins().split(",");
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/api/**")
                        .allowedOrigins(origins)
                        .allowedMethods("GET");
            }
        };
    }
}
