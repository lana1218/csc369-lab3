package csc369;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class GroupByCountryURLCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for User file
    public static class MapperImpl extends Mapper<LongWritable, Text, Text, Text> {


	@Override
        public void map(LongWritable idk, Text value, Context context)  throws IOException, InterruptedException {

		context.write(value, new Text(""));

		}
	}

	public static class SortTime extends WritableComparator {

		//Constructor.

		protected SortTime() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			Text k1 = (Text)w1;
			Text k2 = (Text)w2;

			String country1 = k1.toString().split("\t")[0];
			String country2 = k2.toString().split("\t")[0];

			Integer count1 = Integer.valueOf(k1.toString().split("\t")[2]);
			Integer count2 = Integer.valueOf(k2.toString().split("\t")[2]);

			// comparing counts if same country
			if(country1.equals(country2)) {
				return -1 * count1.compareTo(count2);
			}

			// comparing country to country
			else{
				return country1.compareTo(country2);
			}
		}
	}


    //  Reducer: just one reducer class to perform the "join"
    public static class ReducerImpl extends  Reducer<Text, Text, Text, Text> {

	private IntWritable result = new IntWritable();


	    public void reduce(Text key, Text values, Context context)  throws IOException, InterruptedException {
		context.write(key, values);
	}
    } 


}
