package com.wqj.storm.base.baseconsumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.concurrent.*;

/**
 * @Auther: wqj
 * @Date: 2018/6/5 15:06
 * @Description: 基础kafka消费者
 */
public class BaseConsumer implements Runnable {
    private ExecutorService executorService;

    private List<KafkaStream<byte[], byte[]>> streams;

    private KafkaStream<byte[], byte[]> stream;

    private Integer patitions;

    public BaseConsumer(String zkHost, String topic, String groupId, Integer threadNum, Integer patitions) throws Exception {
        Properties props = new Properties();
        this.patitions = patitions;
        if (StringUtils.isBlank(groupId)) {
            groupId = "com.wqj.storm.base.baseconsumer.BaseConsumer.BaseConsumer";
        }
        if (threadNum == null || threadNum.intValue() <= 0) {
            threadNum = 1;
        }
        if (threadNum.intValue() >= 5) {
            threadNum = 5;
        }
        //判断list是否为空
        if (StringUtils.isBlank(topic)) {

            throw new Exception("topic传入的值为空");
        }
        if (patitions == null) {
            patitions = 1;
        }
        if (StringUtils.isBlank(zkHost)) {
            zkHost = "master:2181,slave1:2181,slave2:2181";
        }
        props.put("group.id", groupId);

        props.put("zookeeper.connect", zkHost);
//        props.put("auto.offset.reset", "largest");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("partition.assignment.strategy", "roundrobin");
        ConsumerConfig config = new ConsumerConfig(props);

        //只要ConsumerConnector还在的话，consumer会一直等待新消息，不会自己退出
        ConsumerConnector consumerConn = Consumer.createJavaConsumerConnector(config);
        //定义一个map
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//        这个类是整体Consumer的核心类，首先要初始化ZookeeperConsumerConnector的伴生对象
//        关于伴生对象请大家查看scala语法，实际就是一个静态对象，每一个class都要有一个伴生对象，像我们的静态方法都要定义在这里面，
//        在createMessageStreams中，topicCountMap主要是消费线程数，这个参数和partition的数量有直接有关系
        //建议和pation数量保持一直,或者成倍数关系,每次消费多少条
        topicCountMap.put(topic, patitions);
        //Map<String, List<KafkaStream<byte[], byte[]>> 中String是topic， List<KafkaStream<byte[], byte[]>是对应的流
        Map<String, List<KafkaStream<byte[], byte[]>>> topicStreamsMap = consumerConn.createMessageStreams(topicCountMap);
        //取出 `topic` 对应的 streams streams.size对应的是patitions,相等的
        this.streams = topicStreamsMap.get(topic);
        //创建一个容量为4的线程池
        this.executorService = Executors.newFixedThreadPool(threadNum);

    }

    private BaseConsumer(KafkaStream<byte[], byte[]> stream) {
        this.stream = stream;

    }

    //策略 ,将数据刷到redis中,从redis中while获取
    public void getConsumerMessage() throws Exception {


        //不选择用callable  因为用Future.get()/f.get(3, TimeUnit.SECONDS);时候会阻塞
//        List<Future> list=new ArrayList<Future>();
//        for (int i = 0; i <patitions ; i++) {
//            executorService.submit(new Callable() {
//                public Object call() throws Exception {
//                    return null;
//                }
//            });
//        }
//        Future f;

        //开启三个线程,刚好与patition一致
        for (int i = 0; i < patitions; i++) {
            executorService.execute(new BaseConsumer(streams.get(i)));
        }
    }


    public void run() {
        /**
         * 不停地从stream读取新到来的消息，在等待新的消息时，hasNext()会阻塞
         * 如果调用 `ConsumerConnector#shutdown`，那么`hasNext`会返回false
         * */
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> data = it.next();
            //主题
            String topic = data.topic();
            //分区id
            int partition = data.partition();
            //偏移量
            long offset = data.offset();
            String msg = new String(data.message());

        }
    }
}
