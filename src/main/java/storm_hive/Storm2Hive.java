package storm_hive;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * storm和hive集成比较麻烦，不适合word_segmentation包里一起做联合测试，
 * 而且storm和HDFS集成很简单，可以直接storm-hdfs然后load到hive表
 *
 *storm跟hive集成，需要修改hive配置，包括开启自动分区、设置metadate的uris、设置jdbc以及开启hive.in.test（参考文件为同包下hive-site.xml）；
 * 确保实际环境的hive版本和代码中的jar包版本一致；确保metadate和hiveserver2开启
 *
 * 本测试的hive建表语句
 * create table demo (id int,name string,sex string) partitioned by (age int) clustered by (id) into 3 buckets stored as orc tblproperties ("orc.compress"="NONE",'transactional'='true');
 *
 * storm-hive集成真的很烦，稍不注意就会失败，而且调错更烦，有兴趣的可以自己测试，希望你能成功，哈哈
 */
public class Storm2Hive {
    static class Storm_Hive_Spout extends BaseRichSpout {
        SpoutOutputCollector spoutOutputCollector;
        String[] name = {"aa","bb","cc","dd","ee","ff","gg","hh"};
        String[] sex = {"man","woman"};
        int[] id = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};

        Random random = new Random();

        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector=spoutOutputCollector;
        }

        public void nextTuple() {
            Utils.sleep(1000);

            String s = name[random.nextInt(name.length)];
            String sex1 = sex[random.nextInt(sex.length)];
            int id1 = id[random.nextInt(id.length)];
            spoutOutputCollector.emit(new Values(id1,s,sex1,"18"));
            System.out.println(""+id1+":"+s+":"+sex1);

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id","name","sex","age"));
        }
    }

    public static void main(String[] args) {


        DelimitedRecordHiveMapper delimitedRecordHiveMapper = new DelimitedRecordHiveMapper();//映射字段，spout那边发来的
        delimitedRecordHiveMapper.withColumnFields(new Fields("id","name","sex"))
                .withPartitionFields(new Fields("age"));

        HiveOptions hiveOptions = new HiveOptions("thrift://localhost:9083","default","demo",delimitedRecordHiveMapper);
        hiveOptions.withTxnsPerBatch(10)
                .withBatchSize(20)
                .withIdleTimeout(10);

        HiveBolt hiveBolt = new HiveBolt(hiveOptions);

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spout",new Storm_Hive_Spout());
        topologyBuilder.setBolt("bolt",hiveBolt).shuffleGrouping("spout");

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("go",new Config(),topologyBuilder.createTopology());

    }

}
