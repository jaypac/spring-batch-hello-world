package com.hello.batch;

import com.hello.batch.entity.CustomerCredit;
import com.hello.batch.repositories.CustomerCreditCrudRepository;
import com.hello.batch.repositories.CustomerCreditPagingAndSortingRepository;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.persistenceunit.DefaultPersistenceUnitManager;
import org.springframework.orm.jpa.persistenceunit.PersistenceUnitManager;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.Map;

@Configuration
@Import(DataSourceConfiguration.class)
@EnableBatchProcessing(isolationLevelForCreate = "ISOLATION_DEFAULT", transactionManagerRef = "jpaTransactionManager")
@EnableJpaRepositories
public class JpaRepositoryJobConfiguration {

    @Bean
    @StepScope
    public RepositoryItemReader<CustomerCredit> itemReader(@Value("#{jobParameters['credit']}") Double credit,
                                                           CustomerCreditPagingAndSortingRepository repository) {
        return new RepositoryItemReaderBuilder<CustomerCredit>().name("itemReader")
                .pageSize(2)
                .methodName("findByCreditGreaterThan")
                .repository(repository)
                .arguments(BigDecimal.valueOf(credit))
                .sorts(Map.of("id", Sort.Direction.ASC))
                .build();
    }

    @Bean
    public RepositoryItemWriter<CustomerCredit> itemWriter(CustomerCreditCrudRepository repository) {
        return new RepositoryItemWriterBuilder<CustomerCredit>().repository(repository).methodName("save").build();
    }

    @Bean
    public Job job(JobRepository jobRepository, JpaTransactionManager jpaTransactionManager,
                   RepositoryItemReader<CustomerCredit> itemReader, RepositoryItemWriter<CustomerCredit> itemWriter) {
        return new JobBuilder("ioSampleJob", jobRepository)
                .start(new StepBuilder("step1", jobRepository)
                        .<CustomerCredit, CustomerCredit>chunk(2, jpaTransactionManager)
                        .reader(itemReader)
                        .processor(new CustomerCreditIncreaseProcessor())
                        .writer(itemWriter)
                        .build())
                .build();
    }

    // Infrastructure beans

    @Bean
    public JpaTransactionManager jpaTransactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }

    @Bean
    public EntityManagerFactory entityManagerFactory(PersistenceUnitManager persistenceUnitManager,
                                                     DataSource dataSource) {
        LocalContainerEntityManagerFactoryBean factoryBean = new LocalContainerEntityManagerFactoryBean();
        factoryBean.setDataSource(dataSource);
        factoryBean.setPersistenceUnitManager(persistenceUnitManager);
        factoryBean.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
        factoryBean.afterPropertiesSet();
        return factoryBean.getObject();
    }

    @Bean
    public PersistenceUnitManager persistenceUnitManager(DataSource dataSource) {
        DefaultPersistenceUnitManager persistenceUnitManager = new DefaultPersistenceUnitManager();
        persistenceUnitManager.setDefaultDataSource(dataSource);
        persistenceUnitManager.afterPropertiesSet();
        return persistenceUnitManager;
    }


}
