package timer;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sker on 17-12-13.
 */
public class TimerTest {
    //定义spout
    static class TimerSpout extends BaseRichSpout {
        SpoutOutputCollector collector;

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        public void nextTuple() {
            Utils.sleep(1000);
            collector.emit(new Values("我是小溪流 永远向前流"));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }
    }

    static class TimerBolt extends BaseBasicBolt {
        //定义一个计数器，方便后面理解
        int m = 0;

        public void execute(Tuple input, BasicOutputCollector collector) {
            //匹配事务
            if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)) {
                //打印这个事务和它的值
                System.out.println(input.getSourceComponent() + input.getValue(0));
                m += 1;//计数器自增
                collector.emit(new Values(m));
            } else {
                System.out.println(input.getValue(0));
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("num"));
        }
    }

    static class TimerBolt1 extends BaseBasicBolt {
        @Override
        public Map<String, Object> getComponentConfiguration() {//定义局部定时器需要重写的方法。返回定时器就可以。
            //优先级问题，局部的定时器优先级大于全局定时器。
            HashMap<String, Object> stringObjectHashMap = new HashMap<String, Object>();
            stringObjectHashMap.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
            return stringObjectHashMap;
        }

        public void execute(Tuple input, BasicOutputCollector collector) {
            //匹配事务
            if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)) {
                //以另外的格式打印，表示这个事务是局部的
                System.out.println("===>" + input.getSourceComponent() + input.getValue(0));
            } else {
                System.out.println(input.getValue(0));
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {

        TopologyBuilder tbq = new TopologyBuilder();
        tbq.setSpout("myspout", new TimerSpout());

        tbq.setBolt("mybolt", new TimerBolt()).shuffleGrouping("myspout");
        tbq.setBolt("mybolt1", new TimerBolt1()).shuffleGrouping("mybolt");

        /*
         * 注掉上面两行用这一行来验证局部与全局优先级的问题
         * 其实验证的意义不大，因为方法是重写的，必然是局部的定时器优先级大于全局定时器。
         * 需要验证的去复习java吧，哈哈哈哈
         */
//        tbq.setBolt("mybolt1",new TimerBolt1()).shuffleGrouping("myspout");

        LocalCluster cluster = new LocalCluster();
        Map config = new Config();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);//设置定时器，每五秒发送一次系统级别的
        //tuple。。这个是全局的。

        cluster.submitTopology("myTopology", config, tbq.createTopology());

        /*
         运行结果如下：
         我是小溪流 永远向前流
         我是小溪流 永远向前流
         我是小溪流 永远向前流
         我是小溪流 永远向前流
         __system5
         1
         我是小溪流 永远向前流
         我是小溪流 永远向前流
         我是小溪流 永远向前流
         我是小溪流 永远向前流
         我是小溪流 永远向前流
         __system5
         2
         ===>__system10
         我是小溪流 永远向前流
         我是小溪流 永远向前流
         我是小溪流 永远向前流
         我是小溪流 永远向前流
         我是小溪流 永远向前流
         __system5
         3
         我是小溪流 永远向前流
         我是小溪流 永远向前流
         我是小溪流 永远向前流
            ...  ...
         */

    }


}
