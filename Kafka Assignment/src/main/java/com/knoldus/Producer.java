package com.knoldus;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class Producer {
    public static void main(String[] args){
        // For example 192.168.1.1:9092,192.168.1.2:9092
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.knoldus.UserSerializer");

        KafkaProducer<String, User> kafkaProducer = new KafkaProducer<>(properties);
        try{
            ObjectMapper mapper= new ObjectMapper();
            Random rand= new Random();
            for(int i = 1; i <=9; i++){
                User user = new User(i,"Alka",rand.nextInt(10)+20,"MCA");

               System.out.println(mapper.writeValueAsString(user));
                kafkaProducer.send(new ProducerRecord<String,User>("user", String.valueOf(i),user));
            }
            System.out.println("All Messages sent !!");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}