package com.scotthensen.tbx.batch;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.scotthensen.tbx.User;
import com.scotthensen.tbx.UserRepo;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class DBWriter implements ItemWriter<User> 
{
	@Autowired
	private UserRepo userRepo;
	
	@Override
	public void write(List<? extends User> users) throws Exception
	{
		log.info("Saved users: {}", users);
		userRepo.saveAll(users);
	}
}
