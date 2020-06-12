package com.ymy.activemq.jms.producer;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageProducer;

import javax.jms.*;
import java.util.UUID;

/**
 * ActiveMQ生产者 Destination--->Queue
 */
public class Produce {

    public static final String ACTIVEMQ_URL = "tcp://192.168.110.132:61616";
    public static final String QUEUE_NAME = "queue01";

    public static void main(String[] args) throws JMSException {
        //1、按照给定的URL和默认的用户名密码,创建ActiveMQConnectionFactory
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
        activeMQConnectionFactory.setBrokerURL(ACTIVEMQ_URL);

        //2、通过ConnectionFactory获取Connection,并且启动访问
        Connection connection = activeMQConnectionFactory.createConnection();
        connection.start();

        //3、通过Connection创建Session
        //createSession()有两个参数：第一个叫事务；第二个叫签收。
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、通过session创建Destination(Queue/Topic)
        Queue queue = session.createQueue(QUEUE_NAME);

        //5、通过session创建消息的生产者
        ActiveMQMessageProducer activeMQMessageProducer = (ActiveMQMessageProducer) session.createProducer(queue);
//        activeMQMessageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);

        //6、通过使用MessageProducer生产3条消息发送到MQ的队列中
        for (int i = 1; i <= 3; i++) {
            //7、创建消息
            TextMessage textMessage = session.createTextMessage("********message：" + i + "*********");
            //8、通过MessageProducer将消息发送给MQ
            activeMQMessageProducer.send(textMessage);
        }

        //9、关闭资源 从下到上依次关闭
        activeMQMessageProducer.close();
        session.close();
        connection.close();

        System.out.println("************消息发送到MQ完成****************");
    }
}
