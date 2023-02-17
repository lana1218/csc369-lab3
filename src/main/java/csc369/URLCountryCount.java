package csc369;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class URLCountryCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for User file
    public static class MapperImpl extends Mapper<LongWritable, Text, Text, Text> {

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
        public void map(LongWritable idk, Text value, Context context)  throws IOException, InterruptedException {

		String hostname = value.toString().split(" ")[0];
		String url = value.toString().split(" ")[6];

		// hostname, corresponding country
		context.write(new Text(url), new Text(country.get(hostname)));

		}
	} 


    //  Reducer: just one reducer class to perform the "join"
    public static class ReducerImpl extends  Reducer<Text, Text, Text, Text> {

	private Text result = new Text();


	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {

			HashSet<String> uniqueCountries = new HashSet<>();

			// iterating through values to put countries
			for(Text v : values){
				uniqueCountries.add(v.toString());;
			}

			List<String> sortedList = new ArrayList<>(uniqueCountries);
			Collections.sort(sortedList);

			String sortedListString = String.join(", ", sortedList);

			result.set(sortedListString);
			context.write(key, result);
	}
    } 


}
