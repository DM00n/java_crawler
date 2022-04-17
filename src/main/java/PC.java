import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

public class PC {
    PC() {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setUsername("rabbitmq");
        connectionFactory.setPassword("rabbitmq");
    }
    ConnectionFactory connectionFactory;
    private Document getDoc(String url){
        while(true){
            HttpGet request = new HttpGet(url);
            int msTimeout = 3000;
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectionRequestTimeout(msTimeout)
                    .setConnectTimeout(msTimeout)
                    .setSocketTimeout(msTimeout)
                    .build();
            request.setConfig(requestConfig);
            try {
                CloseableHttpClient httpClient = HttpClients.createDefault();
                CloseableHttpResponse response = httpClient.execute(request);
                int status = response.getStatusLine().getStatusCode();
                if (status >= 200 && status < 300) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        String result = EntityUtils.toString(entity);
                        return Jsoup.parse(result);
                    }
                } else if (status >= 300 && status < 400){
                    response.close();
                    httpClient.close();
                    url = response.getLastHeader("Location").getValue();
                }
                else if (status > 400){
                    response.close();
                    httpClient.close();
                    break;
                }
            } catch (Exception e){
                System.err.println(e);
                return null;
            }
        }
        return null;
    }

    public void produce() throws InterruptedException, IOException, TimeoutException {
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        String URL = "https://lenta.ru";
        Document doc = getDoc(URL);
        if (doc == null) return;
        String cssQuery = "a[href*=/news/]";
        Elements elements = doc.select(cssQuery);
        for (Element aElement : elements) {
            String href = aElement.attr("href");
            if (href.startsWith("/news")){
                href = URL + href;
                channel.basicPublish("", Main.QUEUE_NAME, null, href.getBytes());
            }
        }
        channel.close();
        connection.close();
    }

    void consume(String name) throws InterruptedException, IOException, TimeoutException {
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        //while (channel.messageCount(Main.QUEUE_NAME) > 0){
        while (true){
            if (channel.messageCount(Main.QUEUE_NAME) == 0) continue;
            String url = new String(channel.basicGet(Main.QUEUE_NAME, true).getBody(), StandardCharsets.UTF_8);
            Document doc = getDoc(url);
            if (doc == null) continue;
            String URL = "URL: " + url + '\n';
            String title =  "Title: " + doc.getElementsByClass("topic-body__title").text() + '\n';
            String text = "Text: " + doc.getElementsByClass("topic-body__content-text").text() + '\n';
            String time = "Time: " + doc.getElementsByClass("topic-header__time").text() + '\n';
            String author = "Author: " + doc.getElementsByClass("topic-authors__author").text() + '\n';
            System.out.println(name+URL+title+text+time+author);
        }
        //channel.close();
        //connection.close();
    }
}
