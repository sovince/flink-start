package com.sovince.project;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Created by vince
 * Email: so_vince@outlook.com
 * Data: 2019/6/25
 * Time: 19:11
 * Description:
 */
public class MockKafkaProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", Constants.BOOTSTRAP_SERVER);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = Constants.TOPIC;
        while (true){

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder
                    .append("sovince").append("\t")
                    .append("CN").append("\t")
                    .append(getLevel()).append("\t")
                    .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("\t")
                    .append(getIp()).append("\t")
                    .append(getDomain()).append("\t")
                    .append(getTraffic()).append("\t")
            ;

            System.out.println(stringBuilder.toString());
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, stringBuilder.toString());
            producer.send(record);

            Thread.sleep(2000);
        }
    }

    private static String getLevel(){
        String[] levels = new String[]{"M","E"};
        return levels[new Random().nextInt(levels.length)];

    }

    private static String getIp(){
        String[] ips = new String[]{
                "121.167.56.69",
                "121.10.132.10",
                "29.121.121.187",
                "121.121.72.30",
                "121.87.10.156",
                "121.30.98.121",
                "121.121.29.143",
                "30.69.87.124",
                "121.167.56.98",
                "56.121.121.46",
                "121.46.72.121",
                "121.121.143.121"
        };
        return ips[new Random().nextInt(ips.length)];
    }

    private static String getDomain(){
        String[] strings = {
                "v2.go2yd.com",
                "v3.go2yd.com",
                "v4.go2yd.com",
                "vmi.go2yd.com"
        };
        return strings[new Random().nextInt(strings.length)];
    }
    private static long getTraffic(){
        return new Random().nextInt(100000);
    }
}
