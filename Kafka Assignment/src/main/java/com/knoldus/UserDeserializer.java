package com.knoldus;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Map;

public class UserDeserializer<T> implements Deserializer<T>{

    @Override public void close() {

    }

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
    }

    @Override
    public T deserialize(String arg0, byte[] arg1) {

        ObjectMapper mapper = new ObjectMapper();
        User user = null;
        try {
            user = mapper.readValue(arg1, User.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return (T) user;
    }



}

