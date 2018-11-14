package test;


import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyListener implements UpdateListener {

    public static Producer setprod (){

        Properties propsprod = new Properties();
        propsprod.put("bootstrap.servers", "localhost:9092,localhost:9093");
        propsprod.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        propsprod.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(propsprod);
        return producer;
    }

    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        try {
            if (newEvents == null) {

                return;
            }
            EventBean event = newEvents[0];
            String ausgabe =(" Sensor 7 " + event.get("sensor7")+" Sensor 8 " + event.get("sensor8"));
            System.out.println(ausgabe);
            ProducerRecord<String, String> record;
            record = new ProducerRecord<String,String>("PJSCEP",ausgabe);
            setprod().send(record);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}