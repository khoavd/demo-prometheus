package com.example.demo.repo;


import com.example.demo.model.MessageModel;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface MessageRepo extends MongoRepository<MessageModel, String> {

    Optional<List<MessageModel>> findByMessage(String msg);

}
