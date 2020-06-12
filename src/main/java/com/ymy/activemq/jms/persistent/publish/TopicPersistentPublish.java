package com.ymy.activemq.jms.persistent.publish;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.time.LocalDateTime;

/**
 * Topic 持久化发布
 */
public class TopicPersistentPublish {
    public static final String ACTIVEMQ_URL = "tcp://192.168.110.132:61616";
    public static final String TOPIC_PERSISTENT_NAME = "topic_persistent";

    public static void main(String[] args) throws JMSException {
        //1、按照给定的URL和默认的用户名密码,创建ActiveMQConnectionFactory
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
        activeMQConnectionFactory.setBrokerURL(ACTIVEMQ_URL);

        //2、通过ConnectionFactory获取Connection
        Connection connection = activeMQConnectionFactory.createConnection();

        //3、通过Connection创建Session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、通过session创建Destination(Queue/Topic)
        Topic topic = session.createTopic(TOPIC_PERSISTENT_NAME);

        //5、通过session创建消息的生产者,并开启Persistent Topic
        MessageProducer messageProducer = session.createProducer(topic);
        messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);

        //6、开启连接连接,启动访问
        connection.start();

        //7、MessageProducer发送消息
        for (int i = 1; i <= 10; i++) {
            TextMessage textMessage = session.createTextMessage("message-persistent" + i + LocalDateTime.now());
            messageProducer.send(textMessage);
        }

        //8、关闭资源
        messageProducer.close();
        session.close();
        connection.close();

        System.out.println("************持久化topic 消息发送成功********");
    }
}
