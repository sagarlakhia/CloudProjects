/*Team of 3
Team Member : Abhitej Date (1001113870),Sagar Lakhia (1001123182), Rasika Dhanurkar(1001110582)
Course Number : CSE 6331-002
Lab Number : Programming Assignment 6
*/
import java.io.IOException;
import java.util.Scanner;
import java.math.*;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WeatherPrediction
{
	public static class avgTempCountMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	  {
			String rowValue = value.toString();
            StringTokenizer st = new StringTokenizer(rowValue);
			//Splitting the data in each document
			String[] rowList=new String[st.countTokens()];
			int index=0;
			 while (st.hasMoreTokens()) {
				 rowList[index]=st.nextToken();
				 index++; 
			}
			String Key="";
			String Value="";
            String season="";
			String year = rowList[0];
            String month = rowList[1];
			//Dividing the dates and assigning them seasons
           if(month.equals("01") || month.equals("02") || month.equals("03") || month.equals("04")){
            	season = "Spring";
            }else if(month.equals("05") || month.equals("06") || month.equals("07") || month.equals("08")){
            	season = "Summer";
            }else if(month.equals("09") || month.equals("10") || month.equals("11") || month.equals("12")){
            	season="Winter";
            }
			//Setting the key as season-year example: summer-1987
            Key = season+"-"+year;
            double temp = Double.parseDouble(rowList[4]);
			//In the dataset -9999 refers to data not available,setting the value to unknown if it encounters -9999
            if(temp == -9999){
            	Value = "unknown";
            }else{
            	Value = Double.toString(temp);
            }
            context.write(new Text(Key), new Text(Value));
	  }
	}
	
	public static class avgTempCountReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			double tempTotal=0;
			int tempCount=0;
			String Values="";
			//Iterating through all the values
		    for(Text val : values)
			{
		    	String str = val.toString();	
		    	if(!str.equals("unknown")){
						double temperature = Double.parseDouble(str);
						tempTotal += temperature;
						tempCount++;
				}
					//Calculating the average for each season in each year
		    		double tempAvg = tempTotal/tempCount;
			    	Values = Double.toString(tempAvg);
			}
		    	
				context.write(key, new Text(Values));
		}
	}
	
	public static class totalAvgTempMapper extends Mapper<Text, Text, Text, Text>
	{
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException
	  {
	    	String keyStr = key.toString();
	    	String[] keyList = keyStr.split("-");
	    	String newKey = keyList[0];
	    	String newValue=keyList[1]+"-"+value;
   			context.write(new Text(newKey), new Text(newValue));
    }
  }

	
	public static class totalAvgTempReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			double totalTemp=0;
			int tmpcount=0;
			String Values="";
			for(Text value : values)
			{
				String valueStr = value.toString();	
				String[] valList = valueStr.split("-");
		    	if(!valList[1].equals("unknown")){
						Values +=valList[0]+"_"+valList[1]+"-";
		    			double temp = Double.parseDouble(valList[1]);
						totalTemp += temp;
						tmpcount++;
				}else{
						Values += valList[0]+"_"+"None"+"-";
				}
			}
			double totalTempAvg = totalTemp/tmpcount;
			Values += Double.toString(totalTempAvg);
 			context.write(key, new Text(Values));
		}
	}

	public static class calWeatherConditionMapper extends Mapper<Text, Text, Text, Text>
	{
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException
	  {
   			context.write(new Text(key), new Text(value));
    }
  }
	
	public static class calWeatherConditionReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String Values="";
			int warmerCount=0, colderCount=0;
			for(Text value : values)
			{
				String valStr = value.toString();
				String[] valList = valStr.split("-");
				String totalAvg = valList[valList.length-1];
				double totalAvgDouble = Double.parseDouble(totalAvg);
		    	for(int index=0; index<valList.length-2;index++){
					String[] seasonList=valList[index].split("_");
		    		if(!seasonList[1].equals("None")){
		    			double listValues = Double.parseDouble(seasonList[1]);
						
		    			if(listValues < totalAvgDouble){
		    				//colderCount++;
							Values=seasonList[0]+"_"+"Colder";
							context.write(key, new Text(Values));
		    			}else{
							Values=seasonList[0]+"_"+"Warmer";
							context.write(key, new Text(Values));
		    				//warmerCount++;
		    			}
		    			//Assigns the season to be colder or warmer depending upon values of warmer and colder variables.
		    		}else{
		    			Values=seasonList[0]+"_"+"Not Found";
						context.write(key, new Text(Values));
		    		}
		    	}
		    	
			}
 			
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		//Take starting time of the system
		long startTime = System.currentTimeMillis();
		//Input path where we have input files
		Path inputPath = new Path(args[0]);
		//Output path where we save first output file
		Path firstOutput = new Path(args[1]+"_1");
		//Output path where we save second output file
		Path secondOutput = new Path(args[1]+"_2");
		//Output path where we save Third output file
		Path thirdOutput = new Path(args[1]+"_3");
		JobConf conf = new JobConf(WeatherPrediction.class);
		Job firstJob = new Job(conf);
		
		Scanner sc = new Scanner(System.in);
	    System.out.println("Please Enter No of Mappers");
		int noOfMappers = sc.nextInt();
		System.out.println("Please Enter No of Reducers");
		int noOfReducers = sc.nextInt();
		//Set Number of mapper Task
		conf.setNumMapTasks(noOfMappers);
		//Set Number of reducer Task
		conf.setNumReduceTasks(noOfReducers);
		firstJob.setJobName("Calculate Average temperature for season");
		firstJob.setJarByClass(WeatherPrediction.class);

		//Set output file key class 
		firstJob.setMapOutputKeyClass(Text.class);
		//Set output file value class 
		firstJob.setMapOutputValueClass(Text.class);
		
		//Set output file key class 
		firstJob.setOutputKeyClass(Text.class);
		//Set output file value class 
		firstJob.setOutputValueClass(Text.class);

		//The Input and Output type for the whole program
		firstJob.setInputFormatClass(TextInputFormat.class);
		firstJob.setOutputFormatClass(TextOutputFormat.class);
		
		//Assigning mapper class to avgTempCountMapper
		firstJob.setMapperClass(avgTempCountMapper.class);

		//Assigning reducer class to avgTempCountReducer
		firstJob.setReducerClass(avgTempCountReducer.class);
		
		//Input and Output file paths for first set of mapper and reducers
		FileInputFormat.addInputPath(firstJob, inputPath);
		FileOutputFormat.setOutputPath(firstJob, firstOutput);
		int waitForCompletion= firstJob.waitForCompletion(true) ? 0 : 1;
		
		if (waitForCompletion == 0)
		{
			Job secondJob = new Job(conf);
			secondJob.setJobName("Calculate total average temperature for Year");
			secondJob.setJarByClass(WeatherPrediction.class);

			//Assigning mapper class to totalAvgTempMapper
			secondJob.setMapperClass(totalAvgTempMapper.class);

			//Assigning reducer class to totalAvgTempReducer
			secondJob.setReducerClass(totalAvgTempReducer.class);
			secondJob.setOutputKeyClass(Text.class);
			secondJob.setOutputValueClass(Text.class);

			//Give previous job output to next job
			secondJob.setInputFormatClass(KeyValueTextInputFormat.class);
			KeyValueTextInputFormat.setInputPaths(secondJob, firstOutput);

			//Set second job output format as text 
			secondJob.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(secondJob, secondOutput);

			//Waiting for job to complete
			waitForCompletion = secondJob.waitForCompletion(true) ? 0 : 2;
			if(waitForCompletion==0){
				Job thirdJob = new Job(conf);
				thirdJob.setJobName("Calculate Season is warmer or Colder");
				thirdJob.setJarByClass(WeatherPrediction.class);

				//Assigning mapper class to calWeatherConditionMapper
				thirdJob.setMapperClass(calWeatherConditionMapper.class);
				thirdJob.setReducerClass(calWeatherConditionReducer.class);

				//Set third job output format as text 
				thirdJob.setOutputKeyClass(Text.class);
				thirdJob.setOutputValueClass(Text.class);

				//Set third job input format as text 
				thirdJob.setInputFormatClass(KeyValueTextInputFormat.class);
				KeyValueTextInputFormat.setInputPaths(thirdJob, secondOutput);

				// Setting up the output format as a text
				thirdJob.setOutputFormatClass(TextOutputFormat.class);
				FileOutputFormat.setOutputPath(thirdJob, thirdOutput);

				//Waiting for job to complete
				waitForCompletion = thirdJob.waitForCompletion(true) ? 0 : 2;
			}
		}
		// end time of the system
		long endTime = System.currentTimeMillis();
		System.out.println("TIme Required " + (endTime-startTime));
		System.exit(waitForCompletion);
		
 	}
}
/*
References:
https://cloud.google.com/hadoop/getting-started
http://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/
*/