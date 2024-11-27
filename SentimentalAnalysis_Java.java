import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

public class MovieReviewAnalysis {

    private static final List<String> positiveWords = new ArrayList<String>();
    private static final List<String> negativeWords = new ArrayList<String>();

    static {
        positiveWords.addAll(Arrays.asList(
            "great", "excellent", "good", "brilliant", "fantastic",
            "amazing", "terrific", "wonderful", "impressive", "exceptional",
            "outstanding", "visual", "engaging", "gripping", "potent",
            "phenomenal", "masterpiece", "remarkable", "intriguing",
            "top-notch", "perfect", "stunning", "fantabulous", "brilliantly",
            "impressively", "satisfying", "captivating", "enjoyable", "greatest"
        ));

        negativeWords.addAll(Arrays.asList(
            "bad", "poor", "failed", "weak", "miserable",
            "fault", "illogical", "disappointing", "lacks", "confused",
            "underdeveloped", "lackluster", "mismatched", "boring", "average",
            "one-time", "flat", "screwed", "disastrous", "dull",
            "mediocre", "unimpressive", "lame", "horrible", "terrible",
            "poorly", "regretful", "awful", "unsatisfactory"
        ));
    }

    public static class SentimentMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable positiveScore = new IntWritable(1);
        private static final IntWritable negativeScore = new IntWritable(-1);
        private Text sentiment = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\\s+");

            int sentimentScore = 0;
            for (String word : words) {
                word = word.replaceAll("[^a-zA-Z]", "").toLowerCase();
                if (positiveWords.contains(word)) {
                    sentimentScore += positiveScore.get();
                } else if (negativeWords.contains(word)) {
                    sentimentScore -= negativeScore.get();
                }
            }

            sentiment.set("SentimentScore");
            context.write(sentiment, new IntWritable(sentimentScore));
        }
    }

    public static class SentimentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalScore = 0;
            for (IntWritable val : values) {
                totalScore += val.get();
            }
            result.set(totalScore);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SentimentAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sentiment Analysis");
        job.setJarByClass(MovieReviewAnalysis.class);
        job.setMapperClass(SentimentMapper.class);
        job.setCombinerClass(SentimentReducer.class);
        job.setReducerClass(SentimentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

