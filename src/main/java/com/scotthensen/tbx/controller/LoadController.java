package com.scotthensen.tbx.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/load")
@Slf4j
public class LoadController 
{
	@Autowired
	JobLauncher jobLauncher;
	
	@Autowired
	Job job;
	
	@GetMapping
	public BatchStatus load() 
			throws JobExecutionAlreadyRunningException, 
			       JobRestartException, 
			       JobInstanceAlreadyCompleteException, 
			       JobParametersInvalidException
	{
		Map<String, JobParameter> parmMap = new HashMap<>();
		parmMap.put("time", new JobParameter(System.currentTimeMillis()));
		JobParameters parms = new JobParameters(parmMap);
		JobExecution jobExecution = jobLauncher.run(job, parms);
		
		log.info("Batch is running...");
		while (jobExecution.isRunning()) {
			log.info(".");
		}
		return jobExecution.getStatus();
	}
	
}
