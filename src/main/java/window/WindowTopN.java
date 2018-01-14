package window;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.storm.windowing.TupleWindow;

import java.util.*;

/**
 * Created by sker on 17-12-13.
 */
public class WindowTopN {
    /**
     * 产生数据的Spout，随机生成指定word并发出
     */
    static class MySpout extends BaseRichSpout {

        String[] words = {"aa","bb","cc","dd","ee","ff","gg"};
        Random random = new Random();
        SpoutOutputCollector collector;
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector=collector;
        }

        public void nextTuple() {
            Utils.sleep(500);
            collector.emit(new Values(words[random.nextInt(words.length)]));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    /**
     * windowBolt，实现窗口操作，并统计指定时间内单位时间间隔内的Top3
     */
    static class MyWindowBolt extends BaseWindowedBolt {

        //定义一个HashMap作wordcount用
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();

        public void execute(TupleWindow inputWindow) {
            //获取窗口内的内容
            List<Tuple> words = inputWindow.get();
            //wordcount
            for (int i = 0; i < words.size(); i++) {
                String word = words.get(i).getString(0);
                Integer count = hashMap.get(word);
                if (count == null)
                    count = 0;
                count++;
                hashMap.put(word, count);
            }
            //这里将map.entrySet()转换成list
            List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(hashMap.entrySet());
            //然后通过比较器来实现排序
            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                //升序排序
                public int compare(Map.Entry<String, Integer> o1,
                                   Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }

            });
            //输出top3
            System.out.println("Top3:");
            for (int i = 0; i < 3; i++) {
                System.out.println("\t" + list.get(i).getKey() + ":" + list.get(i).getValue());
            }
            System.out.println("--------->");

            /*
             *以下代码用于理解window的某些基础操作
             */
//            List<Tuple> tuples = inputWindow.get();
//            List<Tuple> expired = inputWindow.getExpired();//获取到过期的tuple
//            List<Tuple> tuples = inputWindow.getNew();//获取到和上个窗口相比新加进去的tuple
//            System.out.println("滑动了一下！");
//            System.out.println(tuples.size());
//            System.out.println(expired.size());
//            for (Tuple tuple:tuples){
//
//                System.out.println(tuple.getValue(0));
//        }
        }

        public static void main(String[] args) {

            //构建拓扑
            TopologyBuilder topologyBuilder = new TopologyBuilder();
            topologyBuilder.setSpout("spout", new MySpout());
            //指定窗口长度以及滑动间隔
            topologyBuilder.setBolt("bolt", new MyWindowBolt().withWindow(BaseWindowedBolt.Duration.seconds(50), BaseWindowedBolt.Duration.seconds(10))).shuffleGrouping("spout");

        /*
         *以下代码简单理解定义窗口时时间和数量的排列组合
         */
//        topologyBuilder.setBolt("bolt", new MyWindowBolt().withTumblingWindow(BaseWindowedBolt.Count.of(10)))
//              .shuffleGrouping("spout");//这里要注意withTumblingWindow（滑动间隔和窗口长度是一样的）和withWindow的区别，如果忘了点进源码看一下（withWindow是一个tuple滑动一次）
//        topologyBuilder.setBolt("bolt",
////                new new MyWindowBolt().withWindow(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS)))
//                new MyWindowBolt().withWindow(BaseWindowedBolt.Duration.seconds(50), BaseWindowedBolt.Duration.seconds(10)))//时间的两种定义方式
//                .shuffleGrouping("spout");

            LocalCluster localCluster = new LocalCluster();
            Config config = new Config();
            config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 100);//要保证超时时间大于等于窗口长度+滑动间隔长度

            localCluster.submitTopology("a", config, topologyBuilder.createTopology());

        }
    }
}
