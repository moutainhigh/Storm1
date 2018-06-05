package com.wqj.storm.base.baseproducer;

import com.alibaba.fastjson.JSON;
import com.sun.istack.internal.NotNull;
import com.wqj.storm.order.OrderInfo;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Auther: wqj
 * @Date: 2018/6/5 15:06
 * @Description:
 */
public class BaseProducer {
    private ExecutorService executorService;

    private String topic;

    private Producer<String, String> producer;

    /**
     * 消息提供者,初始线程数 用fix的  fix可以控制最大并发数
     *
     * @param topic
     * @param threadNum
     */
    public BaseProducer(String topic, Integer threadNum, String hostsAndPort) {
        if (threadNum == null || threadNum.intValue() <= 0) {
            threadNum = 1;
        }
        if (threadNum.intValue() >= 5) {
            threadNum = 5;
        }


        /**
         * 1、读取配置文件
         */
        Properties props = new Properties();
        /*
         * key.serializer.class默认为serializer.class
		 */
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        /*
         * kafka broker对应的主机，格式为host1:port1,host2:port2
		 */
        if (StringUtils.isBlank(hostsAndPort)) {
            props.put("metadata.broker.list", "master:9092,slave1:9092,slave2:9092");
        } else {
            props.put("metadata.broker.list", hostsAndPort);
        }
        /*
         * request.required.acks,设置发送数据是否需要服务端的反馈,有三个值0,1,-1
		 * 0，意味着producer永远不会等待一个来自broker的ack，这就是0.7版本的行为。
		 * 这个选项提供了最低的延迟，但是持久化的保证是最弱的，当server挂掉的时候会丢失一些数据。
		 * 1，意味着在leader replica已经接收到数据后，producer会得到一个ack。
		 * 这个选项提供了更好的持久性，因为在server确认请求成功处理后，client才会返回。
		 * 如果刚写到leader上，还没来得及复制leader就挂了，那么消息才可能会丢失。
		 * -1，意味着在所有的ISR都接收到数据后，producer才得到一个ack。
		 * 这个选项提供了最好的持久性，只要还有一个replica存活，那么数据就不会丢失
		 */
//        props.put("request.required.acks", "1");
        /*
         * 可选配置，如果不配置，则使用默认的partitioner partitioner.class
		 * 默认值：kafka.producer.DefaultPartitioner
		 * 用来把消息分到各个partition中，默认行为是对key进行hash。
		 */
//        props.put("partitioner.class", "com.wqj.storm.apitemplate.MyPartitoner");
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        this.executorService = Executors.newFixedThreadPool(threadNum);
        this.topic = topic;
        /**
         * 2、通过配置文件，创建生产者
         */
        this.producer = new Producer<String, String>(new ProducerConfig(props));
    }

    public void producerMessage(List<? extends Object> list) throws Exception {
        //判断list是否为空
        if (list == null || list.isEmpty()) {

            throw new Exception("传入的值为空");
        }
        for (final Object message : list) {
            executorService.execute(new Runnable() {
                public void run() {
                    producer.send(new KeyedMessage<String, String>(topic, JSON.toJSONString(message)));
                }
            });
        }
        //任务完成时 ,将线程池关闭,已经在执行的线程不会关闭,但是不会接受新的任务
        executorService.shutdown();
        //判断线程是否完全执行完
        // final   boolean terminated = executorService.isTerminated();

    }

//    /**
//     * 包括 Integer在内的 任何Interger的父类  实践中没什么用
//     * @param list
//     * @throws Exception
//     */
//    public void producerMessage(List<? super Integer> list) throws Exception {
//        //判断list是否为空
//        if (list == null || list.isEmpty()) {
//
//            throw new Exception("传入的值为空");
//        }
//        for (Object message : list) {
//            executorService.execute(new Runnable() {
//                public void run() {
//                    producer.send(new KeyedMessage<String, String>(topic, new OrderInfo().random()));
//                }
//            });
//        }
//
//
//    }


    public static void main(String[] args) throws Exception {
        BaseProducer baseProducer = new BaseProducer("test", null, null);
        String str = new String("1,2,3,4,5,6,7,8,9,10");
        List<String> list = Arrays.asList(str.split(","));
        baseProducer.producerMessage(list);
    }

}
