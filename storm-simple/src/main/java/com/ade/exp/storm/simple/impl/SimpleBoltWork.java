package com.ade.exp.storm.simple.impl;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * Created by liyang on 2017/5/24.
 */
public class SimpleBoltWork implements IRichBolt {

    private OutputCollector collector;

    public void prepare(Map             stormConf,
                        TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        for (int i = 0; i < input.size(); i++) {
            System.out.println("[SimpleBoltWork] " + input.getInteger(i));

        }
        // 确认成功处理一个tuple
        collector.ack(input);
    }

    /**
     * Topology执行完毕的清理工作，比如关闭连接、释放资源等操作都会写在这里
     * 因为这只是个Demo，我们用它来打印我们的计数器
     * */
    public void cleanup() {
        System.out.println("[SimpleBoltWork cleanup]");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
