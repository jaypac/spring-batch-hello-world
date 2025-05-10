package com.hello.batch;

import com.hello.batch.entity.CustomerCredit;
import com.hello.batch.repositories.CustomerCreditCrudRepository;
import com.hello.batch.repositories.CustomerCreditPagingAndSortingRepository;
import jakarta.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.*;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
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
@Slf4j
public class PartitionFileJobConfiguration {

    //@Bean
    //@StepScope
    public RepositoryItemReader<CustomerCredit> itemReader1(@Value("#{jobParameters['credit']}") Double credit,
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
    @StepScope
    public FlatFileItemReader<CustomerCredit> itemReader(@Value("#{stepExecutionContext['fileName']}") String fileName,
                                                         ResourceLoader resourceLoader) {

        log.info("Reading file {}", fileName);
        var fieldSetMapper = new BeanWrapperFieldSetMapper<CustomerCredit>();
        fieldSetMapper.setTargetType(CustomerCredit.class);

        var lineTokenizer = new DelimitedLineTokenizer(",");
        lineTokenizer.setNames("id", "name", "credit");

        var lineMapper = new DefaultLineMapper<CustomerCredit>();
        lineMapper.setFieldSetMapper(fieldSetMapper);
        lineMapper.setLineTokenizer(lineTokenizer);

        return new FlatFileItemReaderBuilder<CustomerCredit>()
                .name("itemReader")
                .lineMapper(lineMapper)
                .resource(resourceLoader.getResource(fileName))
                .build();
    }

    @Bean
    public RepositoryItemWriter<CustomerCredit> itemWriter(CustomerCreditCrudRepository repository) {
        return new RepositoryItemWriterBuilder<CustomerCredit>().repository(repository).methodName("save").build();
    }

    @Bean
    public Partitioner getPartitioner(ResourceLoader resourceLoader) {
        var partitioner = new MultiResourcePartitioner();
        var resource1 = resourceLoader.getResource("classpath:data/delimited1.csv");
        var resource2 = resourceLoader.getResource("classpath:data/delimited2.csv");
        partitioner.setResources(new Resource[]{resource1, resource2});
        return partitioner;
    }


    @Bean
    public Job job(JobRepository jobRepository, JpaTransactionManager jpaTransactionManager,
                   ItemReader<CustomerCredit> itemReader, RepositoryItemWriter<CustomerCredit> itemWriter, Partitioner partitioner) {

        var step1 = getStep1(jobRepository, jpaTransactionManager, itemReader, itemWriter);


        /*PartitionStep partitionStep = new PartitionStep();
        partitionStep.setName("partitionJobStep");
        partitionStep.setJobRepository(jobRepository);

        var stepExecutionSplitter = new SimpleStepExecutionSplitter();
        stepExecutionSplitter.setJobRepository(jobRepository);
        stepExecutionSplitter.setStepName("step1");
        stepExecutionSplitter.setPartitioner(partitioner);

        TaskExecutorPartitionHandler partitionHandler = new TaskExecutorPartitionHandler();
        partitionHandler.setStep(step1);
        partitionHandler.setGridSize(2);
        partitionHandler.setTaskExecutor(new SimpleAsyncTaskExecutor());
        partitionStep.setPartitionHandler(partitionHandler);
        partitionStep.setStepExecutionSplitter(stepExecutionSplitter);
*/

        var partitionStep =  new StepBuilder("partitionJobStep", jobRepository)
                .partitioner("step1", partitioner)
                .step(step1)
                .gridSize(2)
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .build();

        return new JobBuilder("ioSampleJob", jobRepository)
                .start(partitionStep)
                .build();
    }


    private TaskletStep getStep1(JobRepository jobRepository, JpaTransactionManager jpaTransactionManager,
                                 ItemReader<CustomerCredit> itemReader,
                                 RepositoryItemWriter<CustomerCredit> itemWriter) {
        return new StepBuilder("step1", jobRepository)
                .<CustomerCredit, CustomerCredit>chunk(5, jpaTransactionManager)
                .reader(itemReader)
                .processor(new CustomerCreditIncreaseProcessor())
                .writer(itemWriter)
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
