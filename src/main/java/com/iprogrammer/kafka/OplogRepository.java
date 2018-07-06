package com.iprogrammer.kafka;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;

import java.io.Serializable;

@Repository
public interface OplogRepository extends ReactiveMongoRepository<Oplog, Oplog>, Serializable {

}
