package com.ymy.activemq.jms.transaction.producer;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * Destination--->Queue
 * 生产者开启事务
 * 重点：在Session关闭之前提交事务 session.commit();
 */
public class QueueProducerTX {
    public static final String ACTIVEMQ_URL = "tcp://192.168.110.131:61616";

    public static final String QUEUE_TX_NAME = "queue_tx01";

    public static void main(String[] args) throws JMSException {
        //1、按照给定的URL和默认的用户名密码,创建ActiveMQConnectionFactory
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
        activeMQConnectionFactory.setBrokerURL(ACTIVEMQ_URL);

        //2、通过ConnectionFactory获取Connection
        Connection connection = activeMQConnectionFactory.createConnection();

        //3、通过Connection创建Session
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);

        //4、通过session创建Destination(Queue/Topic)
        Queue queue = session.createQueue(QUEUE_TX_NAME);

        //5、通过session创建消息的生产者
        MessageProducer messageProducer = session.createProducer(queue);
        connection.start();

        //6、通过使用MessageProducer生产3条消息发送到MQ的队列中
        try {
            for (int i = 1; i <= 3; i++) {
                TextMessage textMessage = session.createTextMessage("**queue_tx_message**" + i);
                messageProducer.send(textMessage);
            }
            //int i = 10/0;
            //如果没有出错就提交事务
            session.commit();
            System.out.println("*********queue tx 消息发送到MQ成功****************");
        } catch (Exception e) {
            e.printStackTrace();
            //如果try{}语句块中出错,就直接捕获异常,rollback回滚。
            session.rollback();
            System.out.println("***********transaction rollback**********");
        } finally {
            //7、关闭资源
            messageProducer.close();
            session.close();
            connection.close();
        }
    }
}
