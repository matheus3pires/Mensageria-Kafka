package pires.mensageria;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Collections;
import java.util.Properties;

public class PiresEnterprise {
    private JButton comprarButton;
    private JPanel PiresPage;
    private JRadioButton radioButtonIphone;
    private JRadioButton radioButtonSamsung;
    private JRadioButton radioButtonEcho;
    static String value = "";

    public PiresEnterprise() {
        String topicName = "ecommerce-pires";
        Properties propsProducer = new Properties();
        propsProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        comprarButton.addActionListener(new ActionListener() {

            @Override
            public void actionPerformed(ActionEvent e) {

                if (radioButtonIphone.isSelected()) {
                    value = "Iphone 15";
                } else if (radioButtonSamsung.isSelected()) {
                    value = "Samsung s23";
                } else if (radioButtonEcho.isSelected()) {
                    value = "Echo pro";
                }
                KafkaProducer<String, String> producer = new KafkaProducer<>(propsProducer);
                ProducerRecord<String, String> recordProducer = new ProducerRecord<>(topicName, value);
                producer.send(recordProducer);

                System.out.println("Compra iniciada: " + recordProducer.value());
                System.err.println("Aguardando Confirmação...");
            }
        });
    }

    public static void main(String[] args) {
        JFrame quadro = new JFrame("Pires");
        quadro.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        quadro.setContentPane(new PiresEnterprise().PiresPage);
        quadro.setSize(500, 500);
        quadro.setVisible(true);
        quadro.setLocationRelativeTo(null);
        quadro.pack();

        String topicName = "servidor-pires";
        Properties propsConsumer = new Properties();

        propsConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, topicName);
        propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(propsConsumer);
        consumer.subscribe(Collections.singleton(topicName));
        new Thread(() -> {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> recordConsumer : records) {
                    System.out.println("Compra Finalizado. Status => " + recordConsumer.value());
                }
            }
        }).start();
    }
}
