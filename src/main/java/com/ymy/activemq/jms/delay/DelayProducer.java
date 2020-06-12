package com.ymy.activemq.jms.delay;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;

import javax.jms.*;

/**
 * 延迟投递和重复投递
 */
public class DelayProducer {
    public static final String ACTIVEMQ_URL = "tcp://192.168.110.132:61616";
    public static final String QUEUE_NAME = "queue_delay";

    public static void main(String[] args) throws JMSException {
        //1、按照给定的URL和默认的用户名密码,创建ActiveMQConnectionFactory
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
        activeMQConnectionFactory.setBrokerURL(ACTIVEMQ_URL);

        //2、通过ConnectionFactory获取Connection
        Connection connection = activeMQConnectionFactory.createConnection();

        //3、通过Connection创建Session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、通过session创建Destination(Queue/Topic)
        Queue queue = session.createQueue(QUEUE_NAME);

        //5、通过session创建消息的生产者
        MessageProducer messageProducer = session.createProducer(queue);

        long delay = 3 * 1000; //延迟发送的时间
        long period = 4 * 1000; //重复投递的时间间隔
        int repeat = 5; //重复投递的次数
        //6、发送消息
        TextMessage textMessage = session.createTextMessage("***Hello ActiveMQ Delay Send****");
        textMessage.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay);
        textMessage.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, period);
        textMessage.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, repeat);
        messageProducer.send(textMessage);

        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //7、关闭资源
        messageProducer.close();
        session.close();
        connection.close();

        System.out.println("延迟发送到MQ成功！！！");

    }
}
