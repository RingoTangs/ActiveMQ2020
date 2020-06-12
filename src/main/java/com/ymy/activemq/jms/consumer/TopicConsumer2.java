package com.ymy.activemq.jms.consumer;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.IOException;

public class TopicConsumer2 {
    public static final String ACTIVEMQ_URL = "tcp://192.168.110.131:61616";

    public static final String TOPIC_NAME = "topic01";

    public static void main(String[] args) throws JMSException, IOException {

        System.out.println("**************我是消费者02*****************");

        //1、按照给定的URL和默认的用户名密码,创建ActiveMQConnectionFactory
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
        activeMQConnectionFactory.setBrokerURL(ACTIVEMQ_URL);

        //2、通过ConnectionFactory获取Connection,并且启动访问
        Connection connection = activeMQConnectionFactory.createConnection();
        connection.start();

        //3、通过Connection创建Session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、通过session创建Destination(Queue/Topic)
        Topic topic = session.createTopic(TOPIC_NAME);

        //5、通过Session创建消费者
        MessageConsumer messageConsumer = session.createConsumer(topic);

        //6、通过监听的方式来消费消息
        messageConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                if (null != message && message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    try {
                        System.out.println("*******消费者接收到消息：" + textMessage.getText() + "*********");
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        System.in.read();

        messageConsumer.close();
        session.close();
        connection.close();
    }
}
