package com.hello.batch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;


@SpringJUnitConfig(JpaRepositoryJobConfiguration.class)
@SpringBatchTest
class JpaFunctionalTests {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    @BeforeEach
    public void setup(@Autowired Job jobUnderTest) {
        this.jobLauncherTestUtils.setJob(jobUnderTest); // this is optional if the job is unique
        this.jobRepositoryTestUtils.removeJobExecutions();
    }

    @Test
    public void testMyJob() throws Exception {
        // given
        JobParameters jobParameters = new JobParametersBuilder().addDouble("credit", 10000D).toJobParameters();

        // when
        JobExecution jobExecution = this.jobLauncherTestUtils.launchJob(jobParameters);

        // then
        Assertions.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
    }


}

