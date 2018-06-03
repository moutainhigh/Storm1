package com.wqj.storm.base;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * @Auther: wqj
 * @Date: 2018/6/2 18:20
 * @Description:
 */
public class MykafkaBolt1 extends BaseRichBolt {

    OutputCollector collector = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
    }

    public void execute(Tuple tuple) {
        Object input=  tuple.getValue(0);

        collector.emit(new Values("1"));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
