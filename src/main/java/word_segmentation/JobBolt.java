package word_segmentation;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.List;

/**
 * 用来进行中文分词操作，具体中文分词相关内容可以百度word分词器
 * Created by sker on 17-11-13.
 */
public class JobBolt {


    static class GetDocument extends BaseBasicBolt{

        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String string = null;
            try {
                string = new String((byte[]) tuple.getValue(0),"utf-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            try {
                Document parse = Jsoup.parse(new URL(string), 10000);
                Elements p = parse.select("p");
                for(String str:p.eachText()){
                    basicOutputCollector.emit(new Values(str));
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("str"));
        }
    }

    static class StringToWordCount extends BaseBasicBolt{

        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String string = tuple.getString(0);
            List<Word> seg = WordSegmenter.seg(string);
            for (Word word:seg){
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                System.out.println(word);
                basicOutputCollector.emit(new Values(word.getText(),1));
            }

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word","count"));
        }
    }
}
