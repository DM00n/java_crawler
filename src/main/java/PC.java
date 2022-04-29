import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
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
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
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
                channel.basicPublish("", Main.LINK_QUEUE, null, href.getBytes());
            }
        }
        channel.close();
        connection.close();
    }

    void consume() throws InterruptedException, IOException, TimeoutException {
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        while (true){
            if (channel.messageCount(Main.LINK_QUEUE) == 0) continue;
            String url = new String(channel.basicGet(Main.LINK_QUEUE, true).getBody(), StandardCharsets.UTF_8);
            Document doc = getDoc(url);
            if (doc == null) continue;

            Report report = new Report(url,
                    doc.getElementsByClass("topic-body__title").text(),
                    doc.getElementsByClass("topic-body__content-text").text(),
                    doc.getElementsByClass("topic-header__time").text(),
                    doc.getElementsByClass("topic-authors__author").text());
            ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
            String json = ow.writeValueAsString(report);
            channel.basicPublish("", Main.PUT_QUEUE, null, json.getBytes());
        }
    }

    void put() throws IOException, TimeoutException {
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        while (true){
            if (channel.messageCount(Main.PUT_QUEUE) == 0) continue;
            String json = new String(channel.basicGet(Main.PUT_QUEUE, true).getBody(), StandardCharsets.UTF_8);
            Client client = new PreBuiltTransportClient(
                    Settings.builder().put("cluster.name","docker-cluster").build())
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
            String sha256hex = org.apache.commons.codec.digest.DigestUtils.sha256Hex(json);
            IndexResponse response = client.prepareIndex("crawler", "_doc", sha256hex)
                    .setSource(json, XContentType.JSON).get();
        }
    }
}
