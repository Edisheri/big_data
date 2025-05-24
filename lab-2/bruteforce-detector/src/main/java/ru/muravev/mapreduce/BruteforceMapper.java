package ru.muravev.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BruteforceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final String LOG_REGEX = "^([\\d.]+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([^\"]+)\" (\\d{3}) (\\d+)";
    private static final Pattern LOG_PATTERN = Pattern.compile(LOG_REGEX);

    private static final IntWritable ONE = new IntWritable(1);

    private Text ip = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        Matcher matcher = LOG_PATTERN.matcher(value.toString());
        if (matcher.find()) {
            ip.set(matcher.group(1));
            context.write(ip, ONE);
        }
    }
}
