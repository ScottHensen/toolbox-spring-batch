package com.scotthensen.tbx.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import com.scotthensen.tbx.User;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig 
{
	@Bean
	public Job job( JobBuilderFactory  jobBuilderFactory,
			        StepBuilderFactory stepBuilderFactory,
			        ItemReader<User>   itemReader,
			        ItemProcessor<User,User> itemProcessor,
			        ItemWriter<User>   itemWriter          ) 
	{	
		Step step = 
				stepBuilderFactory
				.get("ETL-file-load")
				.<User,User>chunk(100)
				.reader(itemReader)
				.processor(itemProcessor)
				.writer(itemWriter)
				.build();
		
		return  jobBuilderFactory
				.get("ETL-load")
				.incrementer(new RunIdIncrementer())
				.start(step)
				.build();

// multi-step job
//		return jobBuilderFactory
//				.get("ETL-load")
//				.incrementer(new RunIdIncrementer())
//				.flow(step1)
//				.next(step2)
//				.build();
	}
	
	@Bean
	public FlatFileItemReader<User> fileItemReader(@Value("${input}") Resource resource)
	{
		FlatFileItemReader<User> reader = new FlatFileItemReader<>();
		reader.setResource(resource);
		reader.setName("CSV-reader");
		reader.setLinesToSkip(1);
		reader.setLineMapper(lineMapper());
		return reader;
	}

	@Bean
	public LineMapper<User> lineMapper() 
	{
		DefaultLineMapper<User> lineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		
		tokenizer.setDelimiter(",");
		tokenizer.setStrict(false);
		tokenizer.setNames(new String[] {"id", "name", "dept", "salary"} );
		
		BeanWrapperFieldSetMapper<User> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
		fieldSetMapper.setTargetType(User.class);
		
		lineMapper.setLineTokenizer(tokenizer);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		
		return lineMapper;
	}
}
