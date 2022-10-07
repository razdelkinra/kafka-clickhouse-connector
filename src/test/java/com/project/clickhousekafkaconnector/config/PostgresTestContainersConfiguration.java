package com.project.clickhousekafkaconnector.config;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.Rule;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;


@TestConfiguration
public class PostgresTestContainersConfiguration implements InitializingBean {

    public static final String DB_NAME = "postgres";
    public static final String USER_NAME = "sa";
    public static final String TEST_PASSWORD = "sa";

    @Rule
    public static GenericContainer postgreSQLContainer = new PostgreSQLContainer(DockerImageName.parse("postgres:12.11").asCompatibleSubstituteFor("postgres"))
            .withDatabaseName(DB_NAME)
            .withUsername(USER_NAME)
            .withPassword(TEST_PASSWORD);

    @Override
    public void afterPropertiesSet() {
        postgreSQLContainer.start();
    }

    @Bean
    @Primary
    public DataSource dataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl("jdbc:postgresql://localhost:" + postgreSQLContainer.getMappedPort(5432) + "/" + DB_NAME);
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUsername(USER_NAME);
        dataSource.setPassword(TEST_PASSWORD);
        return dataSource;
    }

    @Bean
    @Primary
    public ConnectionFactory connectionFactory() {
        return ConnectionFactories.get(
                ConnectionFactoryOptions.builder()
                        .option(DRIVER, "postgresql")
                        .option(HOST, "localhost")
                        .option(USER, USER_NAME)
                        .option(PORT, postgreSQLContainer.getMappedPort(5432))
                        .option(PASSWORD, TEST_PASSWORD)
                        .option(DATABASE, DB_NAME)
                        .build());
    }
}
