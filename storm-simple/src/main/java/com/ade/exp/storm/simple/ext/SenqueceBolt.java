package com.ade.exp.storm.simple.ext;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 *
 * Created by liyang on 2017/5/24.
 */
public class SenqueceBolt implements IRichBolt {

    private OutputCollector collector;

    // 仅在bolt开始处理元组之前调用
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    // 处理输入的单个元组
    @Override
    public void execute(Tuple tuple) {
    String word = (String) tuple.getValue(0);
        String out = "I'm " + word +  "!";
        System.out.println("out=" + out);
        // ack，改为实现IBasicBolt接口 execute后会自动ack
        collector.ack(tuple);
    }

    // 在bolt即将关闭时调用
    @Override
    public void cleanup() {
    }

    // 为bolt声明输出模式
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}