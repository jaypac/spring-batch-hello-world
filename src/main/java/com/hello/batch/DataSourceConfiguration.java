package com.hello.batch;

import org.springframework.boot.autoconfigure.batch.BatchDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.support.JdbcTransactionManager;

import javax.sql.DataSource;

import static org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType.H2;

@Configuration
public class DataSourceConfiguration {

    @Bean
    @BatchDataSource
    public DataSource dataSource() {
        return new EmbeddedDatabaseBuilder().setType(H2)
                .addScript("/org/springframework/batch/core/schema-drop-h2.sql")
                .addScript("/org/springframework/batch/core/schema-h2.sql")
                .addScript("/db/schema.sql")
                .addScript("/db/data.sql")
                .generateUniqueName(true)
                .build();
    }

    @Bean
    public JdbcTransactionManager transactionManager(DataSource dataSource) {
        return new JdbcTransactionManager(dataSource);
    }

}
