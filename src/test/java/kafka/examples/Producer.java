/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;


import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

public class Producer extends Thread
{
  private final kafka.javaapi.producer.Producer<Integer, String> producer;
  private final String topic;
  private final Properties props = new Properties();

  public Producer(String topic) throws ClassNotFoundException,InstantiationException, IllegalAccessException,
          IllegalArgumentException, InvocationTargetException {
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("zk.connect", "localhost:2181");
    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
    this.topic = topic;
  }
  
  public void run() {
    try{
      int messageNo = 1;
      //while(true)
      for(int i = 0;i < 10;i++)
      {
        String messageStr = new String("asdfghj" + messageNo);
        System.out.println(messageStr);
        producer.send(new ProducerData<Integer, String>(topic, messageStr));
        messageNo++;
      }
    }catch (Exception e){
      e.printStackTrace();
    }
  }

}
