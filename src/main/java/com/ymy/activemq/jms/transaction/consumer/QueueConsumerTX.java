package com.ymy.activemq.jms.transaction.consumer;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * Destination--->Queue
 * 消费者开启事务
 */
public class QueueConsumerTX {
    public static final String ACTIVEMQ_URL = "tcp://192.168.110.131:61616";

    public static final String QUEUE_TX_NAME = "queue_tx01";

    public static void main(String[] args) throws JMSException {
        //1、按照给定的URL和默认的用户名密码,创建ActiveMQConnectionFactory
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
        activeMQConnectionFactory.setBrokerURL(ACTIVEMQ_URL);

        //2、通过ConnectionFactory获取Connection
        Connection connection = activeMQConnectionFactory.createConnection();

        //3、通过Connection创建Session
        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);

        //4、通过session创建Destination(Queue/Topic)
        Queue queue = session.createQueue(QUEUE_TX_NAME);

        //5、通过session创建消息的消费者
        MessageConsumer messageConsumer = session.createConsumer(queue);
        connection.start();

        //6、MessageConsumer来消息消息
        try {
            while (true) {
                //7、MessageConsumer(消费者)接收MQ中的消息
                TextMessage textMessage = (TextMessage) messageConsumer.receive(4000);
                if (textMessage != null) {
                    System.out.println("*******消费者接收到消息：" + textMessage.getText() + "*********");
                    textMessage.acknowledge();
                } else {
                    break;
                }
            }
            //int i = 10/0;
            //所有消息都被消费成功
//            session.commit();
        } catch (Exception e) {
            e.printStackTrace();
            session.rollback();
            System.out.println("transaction rollback....");
        } finally {
            messageConsumer.close();
            session.close();
            connection.close();
        }
    }
}
