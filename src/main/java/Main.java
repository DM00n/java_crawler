import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class Main {
    static String LINK_QUEUE = "link_queue";
    static String PUT_QUEUE = "put_queue";
    public static void main(String[] args) throws InterruptedException, IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setUsername("rabbitmq");
        connectionFactory.setPassword("rabbitmq");

        Connection connection = connectionFactory.newConnection();

        Channel channel = connection.createChannel();
        channel.queueDeclare(LINK_QUEUE, false, false, false, null);
        channel.queueDeclare(PUT_QUEUE, false, false, false, null);
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
                pc.consume();
            }
            catch (InterruptedException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        Thread t3 = new Thread(() -> {
            try {
                pc.consume();
            }
            catch (InterruptedException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        Thread t4 = new Thread(() -> {
            try {
                pc.put();
            }
            catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        });

        Thread t5 = new Thread(() -> {
            try {
                pc.get();
            }
            catch (IOException | ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        /*
          t1 - Ищет все ссылки в исходной ссылке, кладёт их в первую очередь
          t2, t3 - Берут из первой очереди ссылки, парсят их и кладут содержимое во вторую очередь
          t4 - Берёт из второй очереди содержимое новостей и кладёт в Эластик
          t5 - Запросы
        */


        /*t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();

        t1.join();
        t2.join();
        t3.join();
        t4.join();
        t5.join();*/
    }
}
