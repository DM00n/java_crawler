import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Main {
    static String QUEUE_NAME = "crawler_queue";
    public static void main(String[] args) throws InterruptedException, IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setUsername("rabbitmq");
        connectionFactory.setPassword("rabbitmq");

        Connection connection = connectionFactory.newConnection();

        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.close();
        connection.close();

        PC pc = new PC();
        Thread t1 = new Thread(() -> {
            try {
                pc.produce();
            }
            catch (InterruptedException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                pc.consume("t2\n");
            }
            catch (InterruptedException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        Thread t3 = new Thread(() -> {
            try {
                pc.consume("t3\n");
            }
            catch (InterruptedException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        });

        t1.start();
        t2.start();
        t3.start();

        t1.join();
        t2.join();
        t3.join();

    }
}
