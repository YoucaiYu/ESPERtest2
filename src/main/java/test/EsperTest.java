package test;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;


public class EsperTest {
    public static void main(String args[]) {
        try {
            Configuration config = new Configuration();
            config.addEventType("SensorEvent",
                    test.SensorEvent.class.getName());
            EPServiceProvider epService = EPServiceProviderManager
                    .getDefaultProvider(config);
            String expression = "select * from SensorEvent where sensor7 =1 and sensor8 =1";

            EPStatement statement = epService.getEPAdministrator().createEPL(expression);
            MyListener listener = new MyListener();
            statement.addListener(listener);

            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");
            KafkaConsumer consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList("PJS4"));
            while (true) {
            ConsumerRecords<String, String> rows = consumer.poll(100);
            for (ConsumerRecord <String,String> record: rows) {

                String event = record.value();
                String[] versuch = event.split(";");
                SensorEvent eventob = new SensorEvent();

                eventob.setDate(versuch[1]);
                eventob.setSensor7(Integer.valueOf(versuch[8]));
                eventob.setSensor8(Integer.valueOf(versuch[9]));
                eventob.setSensor9(Integer.valueOf(versuch[10]));

                epService.getEPRuntime().sendEvent(eventob);

                }

            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}