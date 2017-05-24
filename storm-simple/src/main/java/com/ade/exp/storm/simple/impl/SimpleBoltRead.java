package com.ade.exp.storm.simple.impl;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 *
 * Created by liyang on 2017/5/24.
 */
public class SimpleBoltRead implements IRichBolt {

    private OutputCollector collector;

    public void prepare(Map             stormConf,
                        TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 这是bolt中最重要的方法，每当接收到一个tuple时，此方法便被调用
     * 这个方法的作用就是把文本文件中的每一行切分成一个个单词，并把这些单词发射出去（给下一个bolt处理）
     **/
    public void execute(Tuple input) {
        for (int i = 0; i < input.size(); i++) {
            Integer sentence = input.getInteger(i);
            System.out.println("[SimpleBoltRead] " + sentence);
            collector.emit(new Values(sentence));
        }
        //确认成功处理一个tuple
        collector.ack(input);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("read"));
    }

    public void cleanup() {
        System.out.println("[SimpleBoltRead cleanup]");
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
