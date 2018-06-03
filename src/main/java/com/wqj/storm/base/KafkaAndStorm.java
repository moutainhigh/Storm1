package com.wqj.storm.base;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * @Auther: wqj
 * @Date: 2018/6/1 16:27
 * @Description:
 */
public class KafkaAndStorm {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        BrokerHosts brokerHosts = new ZkHosts("master:2181,slave1:2181,slave2:2181");

        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "test", "", "spout");

//        spoutConfig.socketTimeoutMs = 60 * 1000 ;
//        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()) ;
        //自定义的转换
//        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());



        TopologyBuilder builder = new TopologyBuilder();

        Config conf = new Config();
            conf.setNumWorkers(1);

        //本地模式保存offset
//        spoutConfig.zkServers = new ArrayList<String>(){{
//            add("master");
//            add("slave1");
//            add("slave2");
//        }};
        spoutConfig.zkPort = 2181;

        builder.setSpout("spout", new KafkaSpout(spoutConfig));
        builder.setBolt("bolt", new MykafkaBolt1(),1).shuffleGrouping("spout");


        if (args != null && args.length > 0) {
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            // 这里是本地模式下运行的启动代码。
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("simple", conf, builder.createTopology());
        }
    }
}
