package csc369;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	} else if ("UserMessages".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, UserMessages.UserMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, UserMessages.MessageMapper.class ); 

	    job.setReducerClass(UserMessages.JoinReducer.class);

	    job.setOutputKeyClass(UserMessages.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(UserMessages.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("ReverseKeyValue".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(ReverseKeyValue.ReducerImpl.class);
		job.setMapperClass(ReverseKeyValue.MapperImpl.class);
		job.setSortComparatorClass(ReverseKeyValue.SortIntComparator.class);
		job.setOutputKeyClass(ReverseKeyValue.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(ReverseKeyValue.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("CountryCount".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(CountryCount.ReducerImpl.class);
		job.setMapperClass(CountryCount.MapperImpl.class);
		job.setOutputKeyClass(CountryCount.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(CountryCount.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("CountryURLCount".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(CountryURLCount.ReducerImpl.class);
		job.setMapperClass(CountryURLCount.MapperImpl.class);
		job.setOutputKeyClass(CountryURLCount.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(CountryURLCount.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("GroupByCountryURLCount".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(GroupByCountryURLCount.ReducerImpl.class);
		job.setMapperClass(GroupByCountryURLCount.MapperImpl.class);
		job.setSortComparatorClass(GroupByCountryURLCount.SortTime.class);
		job.setOutputKeyClass(GroupByCountryURLCount.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(GroupByCountryURLCount.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("URLCountryCount".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(URLCountryCount.ReducerImpl.class);
		job.setMapperClass(URLCountryCount.MapperImpl.class);
		job.setOutputKeyClass(URLCountryCount.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(URLCountryCount.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
