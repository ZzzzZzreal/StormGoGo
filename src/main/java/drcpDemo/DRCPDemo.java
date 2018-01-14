package drcpDemo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * Created by sker on 17-12-13.
 */
public class DRCPDemo {

    static class DRPCBolt extends BaseBasicBolt {

        public void execute(Tuple input, BasicOutputCollector collector) {
            Fields fields = input.getFields();
            List<String> strings = fields.toList();
            for(int i=0;i<strings.size();i++){
                System.out.println(strings.get(i)+"!!!!");
            }
            collector.emit(new Values(input.getValue(0)+"!!!!",input.getValue(1)));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id","result"));
        }
    }

    public static void main(String[] args) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        LocalDRPC localDRPC = new LocalDRPC();

        DRPCSpout drpcSpout = new DRPCSpout("test",localDRPC);
        topologyBuilder.setSpout("1",drpcSpout);
        topologyBuilder.setBolt("2",new DRPCBolt()).noneGrouping("1");
        topologyBuilder.setBolt("3",new ReturnResults()).noneGrouping("2");

        Config config = new Config();

        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology("x",config,topologyBuilder.createTopology());

        String execute = localDRPC.execute("test", "word");
//        DRPCRequest test = localDRPC.fetchRequest("test");
//        System.out.println(test.get_request_id());
        System.out.println(execute);

        localCluster.shutdown();
        localDRPC.shutdown();

    }




}
