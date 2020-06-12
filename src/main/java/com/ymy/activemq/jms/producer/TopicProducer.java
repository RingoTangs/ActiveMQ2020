package com.ymy.activemq.jms.producer;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * ActiveMQ生产者 Destination--->Topic
 */
public class TopicProducer {

    public static final String ACTIVEMQ_URL = "tcp://192.168.110.131:61616";

    public static final String TOPIC_NAME = "topic01";

    public static void main(String[] args) throws JMSException {
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

        //5、通过session创建消息的生产者
        MessageProducer messageProducer = session.createProducer(topic);

        //6、通过使用MessageProducer生产3条消息发送到MQ的队列中
        for (int i = 1; i <= 18; i++) {
            //7、创建消息
            TextMessage textMessage = session.createTextMessage("********message：" + i + "*********");
            textMessage.setStringProperty("client-01","vip");
            //8、通过MessageProducer将消息发送给MQ
            messageProducer.send(textMessage);
        }

        //9、关闭资源 从下到上依次关闭
        messageProducer.close();
        session.close();
        connection.close();

        System.out.println("************Topic消息发送到MQ完成****************");

    }
}
