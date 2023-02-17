package csc369;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.*;

public class CountryCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    // Mapper for User file
    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final IntWritable one = new IntWritable(1);

		private HashMap<String, Text> country = new HashMap();
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);

			BufferedReader fileReader = new BufferedReader(new FileReader("input_users/hostname_country.csv"));
			String line = "";

			//Read the file line by line
			while ((line = fileReader.readLine()) != null) {
				//Get all tokens available in line
				String[] parts = line.split(",");
				country.put(parts[0], new Text(parts[1]));
			}
		}
	@Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {

		String hostname = value.toString().split(" ")[0];

		context.write(country.get(hostname), one);

		}
	} 


    //  Reducer: just one reducer class to perform the "join"
    public static class ReducerImpl extends  Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();

	@Override
	    public void reduce(Text key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {
		int sum = 0;
		Iterator<IntWritable> itr = values.iterator();

		while (itr.hasNext()) {
			sum  += itr.next().get();
		}
		result.set(sum);
		context.write(key, result);
	}
    } 


}
