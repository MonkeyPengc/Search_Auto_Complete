
/**
 * Created by chengpeng on 6/4/17.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class LanguageModel {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        int threshold;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            threshold = conf.getInt("threshold", 5);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if ((value == null) || (value.toString().trim().length() == 0)) {
                return;
            }

            //this is cool\t20
            String line = value.toString().trim();
            String wordsPlusCount[] = line.split("\t");
            String words[] = wordsPlusCount[0].split("\\s+");
            int count = Integer.valueOf(wordsPlusCount[wordsPlusCount.length - 1]);

            if ((wordsPlusCount.length < 2) || (count <= threshold)) {
                return;
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                sb.append(words[i]).append(" ");
            }

            String outputKey = sb.toString().trim();
            String outputValue = words[words.length-1];

            if (!((outputKey == null) || (outputKey.length() < 1))) {
                context.write(new Text(outputKey), new Text(outputValue + "=" + count));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

        int k;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 5);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());

            // want to -> [100, [see, go]]
            // want to -> [90, [have]]
            for (Text val : values) {
                String val_expression = val.toString().trim();
                String word = val_expression.split("=")[0].trim();
                int count = Integer.parseInt(val_expression.split("=")[1].trim());

                if (tm.containsKey(count)) {
                    tm.get(count).add(word);
                }

                else {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    tm.put(count, list);
                }
            }

            Iterator<Integer> iter = tm.keySet().iterator();

            for (int j=0; iter.hasNext() && j < k; j++) {
                int keyCount = iter.next();
                List<String> words = tm.get(keyCount);
                for (String word : words) {
                    context.write(new DBOutputWritable(key.toString(), word, keyCount), NullWritable.get());
                    j++;
                }
            }
        }
    }
}


