package miu.bdt.sockets;

import miu.bdt.kafka.IKafkaConstants;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SocketServerKafka implements Runnable {
    int portNumber = 9999;

    public SocketServerKafka(int port) {
        this.portNumber = port;
    }

//    public static void main(String[] args) throws IOException, InterruptedException {
//        PrintWriter pw = new PrintWriter(System.out);
//        new SocketServerKafka(9999).sendNewsToClient(pw);
//    }

    private void sendNewsToClient(PrintWriter out) throws IOException, InterruptedException {
        Consumer<Long, String> consumer = createConsumer();

        int noMessageToFetch = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            if (consumerRecords.count() == 0) {
                noMessageToFetch++;
                if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    break;
                else
                    continue;
            }

            int period = 0;
            for (ConsumerRecord<Long, String> record : consumerRecords) {
//                out.println("Record Key " + record.key());
//                out.println("Record value " + record.value());
//                out.println("Record partition " + record.partition());
//                out.println("Record offset " + record.offset());

                out.println(record.value());

                // simulate stream
                if (period++ % 2 == 0)
                    Thread.sleep(2000);
            }

            //consumer.commitAsync();
        }

        consumer.close();
        System.out.println("The consumer has been closed!");
    }

    public static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConstants.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, IKafkaConstants.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConstants.OFFSET_RESET_EARLIER);

        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(IKafkaConstants.TOPIC_NAME));
        return consumer;
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(portNumber);
             Socket clientSocket = serverSocket.accept();
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
             BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));) {
            sendNewsToClient(out);
        } catch (IOException | InterruptedException e) {
            System.out.println("Exception caught when trying to listen on port "
                    + portNumber + " or listening for a connection");
            System.out.println(e.getMessage());
        }
    }
}
