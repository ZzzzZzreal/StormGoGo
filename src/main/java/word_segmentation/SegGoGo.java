package word_segmentation;

import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

/**
 * storm集成Kafka、Hive、JDBC、HBase、HDFS
 * Created by sker on 17-11-13
 * kafka集成storm，将数据发到JobBolt做中文分词逻辑；
 * 结果发到不同bolt，然后分别存入hive、hbase、mysql和hdfs
 */
public class SegGoGo {

    public static void main(String[] args) {

        //创建一个TopologyBuilder实例
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        LocalCluster localCluster = new LocalCluster();
        Config conf = new Config();

        /**
         * 以下是kafka到storm的逻辑
         */

        //kafka与storm集成需要一个zkHost和一个SpoutConfig
        ZkHosts zkHosts = new ZkHosts("localhost:2181");
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, "hbase", "/storm", "kafka");

        /**
         * 以下代码要做的是storm与HDFS集成
         */

        //kafka与HDFS集成需要一个HDFSBolt,并进行相应参数的设定
        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl("hdfs://localhost:9000/")//设置hdfs的url
                .withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter(","))//设置文件分割符
                .withSyncPolicy(new CountSyncPolicy(10))//同步政策
                .withFileNameFormat(new DefaultFileNameFormat().withPath("/test"))//文件命名格式，参数中设置了文件路径
                .withRotationPolicy(new FileSizeRotationPolicy(1.0f, FileSizeRotationPolicy.Units.KB));//设置滚动生成文件的参数，此处为1k生成一个文件

        /**
         * 以下代码要做的是storm与hbase集成
         */

        //storm与hbase集成
        Config config = new Config();
        Map<String, Object> hbConf = new HashMap<String, Object>();
        hbConf.put("hbase.rootdir","hdfs://localhost:9000/sbsbsbs/hbase/");
        hbConf.put("hbase.zookeeper.quorum", "localhost:2181");
        config.put("hbase.conf", hbConf);

        SimpleHBaseMapper simpleHBaseMapper = new SimpleHBaseMapper()
                .withColumnFamily("cf")
                .withColumnFields(new Fields("word","count"))
                .withRowKeyField("word");

        HBaseBolt hBaseBolt = new HBaseBolt("demo",simpleHBaseMapper)
                .withConfigKey("hbase.conf");


        /**
         * 以下代码要做的是storm与JDBC集成
         */
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/test?useunicode=true&amp;characterencoding=utf-8");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","1327");

        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        String tableName = "seg";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt insertBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
//                .withTableName("seg")     //没卵用
                .withInsertQuery("insert into seg values (?,?)")
                .withQueryTimeoutSecs(30);

        JdbcInsertBolt selectBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withInsertQuery("select word,count(word) from seg group by word")
                .withQueryTimeoutSecs(30);

        /**
         * 构建拓扑
         */
        //kafka到storm的spout，构建拓扑的第一步
        topologyBuilder.setSpout("kafka", new KafkaSpout(spoutConfig));
        //数据进入JobBolt做中文分词处理
        topologyBuilder.setBolt("document",new JobBolt.GetDocument()).shuffleGrouping("kafka");
        topologyBuilder.setBolt("wordCount",new JobBolt.StringToWordCount()).shuffleGrouping("document");
        //数据插入mysql
        topologyBuilder.setBolt("jdbc_insert",insertBolt).shuffleGrouping("wordCount");
        //查询mysql
        topologyBuilder.setBolt("jdbc_select",selectBolt).shuffleGrouping("jdbc_insert");
        //数据存入HDFS
        topologyBuilder.setBolt("hdfs",hdfsBolt).shuffleGrouping("jdbc_select");
        //数据存入HBase
        topologyBuilder.setBolt("hbase",hBaseBolt).shuffleGrouping("wordCount");


        localCluster.submitTopology("SegGoGo",config,topologyBuilder.createTopology());

    }
}





