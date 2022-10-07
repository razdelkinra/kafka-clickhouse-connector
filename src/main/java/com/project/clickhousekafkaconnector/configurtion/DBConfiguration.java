package com.project.clickhousekafkaconnector.configurtion;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;

import javax.sql.DataSource;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

@Configuration
@EnableR2dbcRepositories(basePackages = "com.project.clickhousekafkaconnector.repository")
public class DBConfiguration {

    @Value("${application.db.address}")
    private String address;

    @Value("${application.db.port}")
    private Integer port;

    @Value("${application.db.username}")
    private String user;

    @Value("${application.db.password}")
    private String password;

    @Value("${application.db.database}")
    private String database;

    @Bean
    @ConfigurationProperties(prefix = "spring.postgres")
    public DataSource liquibaseDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    public ConnectionFactory connectionFactory() {
        return ConnectionFactories.get(
                ConnectionFactoryOptions.builder()
                        .option(DRIVER, "postgresql")
                        .option(HOST, address)
                        .option(USER, user)
                        .option(PORT, port)
                        .option(PASSWORD, password)
                        .option(DATABASE, database)
                        .build());
    }

}
