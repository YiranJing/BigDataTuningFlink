package assignment;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.log4j.BasicConfigurator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


public class AverageDepartureDelay {
	
	public static void main(String[] args) throws Exception {
		  
		BasicConfigurator.configure();
		
		//String year = args[0];  // command argument, in this code, I use 2004 temporarily
		
		/****************************
		*** READ IN DATA NEEDED. ***
		****************************/

		// Don't forget to change file path!
		
		final String PATH = "/Users/yiranjing/Desktop/DATA3404/assignment_data_files/";
		
		//final String PATH = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/";
		final ParameterTool params = ParameterTool.fromArgs(args);
		//final String Out_PATH = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/yjin5856/result/";
		String outputFilePath = params.get("output", PATH + "avg_dep_delay.txt");
		// Used for time interval calculation
		SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
		 
	    
	    // obtain handle to execution environment
	    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	    
	    DataSet<Tuple4<String, String,String,String>> flights =
			      env.readCsvFile(PATH + "ontimeperformance_flights_tiny.csv")
			      .includeFields("0100001101") 
			      .ignoreFirstLine() 
			      .ignoreInvalidLines() 
			      .types(String.class,String.class,String.class,String.class); 
	    
	    DataSet<Tuple2<String,String>> aircrafts =
	    		  env.readCsvFile(PATH +"ontimeperformance_aircrafts.csv")
	    		  .includeFields("100000001") 
		          .ignoreFirstLine() 
		          .ignoreInvalidLines() 
		          .types(String.class, String.class); 
	    
	    DataSet<Tuple3<String, String,String>> airlines =
			      env.readCsvFile(PATH +"ontimeperformance_airlines.csv")
			      .includeFields("111") 
			      .ignoreFirstLine() 
			      .ignoreInvalidLines() 
			      .types(String.class,String.class, String.class); 
	    
	    
	    
		/****************************
		*** ACTUAL IMPLEMENTATION ***
		****************************/						

		/****************************
		*	Implementation
		*
		* 1) Filter Processing
		*    a) Filter US airlines 
		*    b) Filter for specific year
		*    c) Filter out cancelled flights, only keep delayed flights, and then compute delay for each flight
		* 
		* 2) Sorted Airline name in ascending order
		* 3) Join three filtered data sets
		* 4) Group by and Aggregate the result using Airline name, for number, sum, min and max delay time 
		* 5) Compute the average time
		****************************/
	    
	// Step 1 a)
	    DataSet<Tuple2<String, String>> USairlines = 
				airlines.filter(new FilterFunction<Tuple3<String, String, String>>() {
				@Override
				public boolean filter(Tuple3<String, String, String> tuple) {
					// Filter for United States
					return tuple.f2.contains("United States"); }    
				})
				.project(0, 1);
	    
	    
	    
	// Step 1 b) 
		DataSet<Tuple1<String>> aircraftsYear =
		    		aircrafts.filter(new FilterFunction<Tuple2<String,String>>() {
		                            public boolean filter(Tuple2<String, String> entry) { return entry.f1.equals("2004"); } 
		            }).project(0); 
	   
		
    // Step 1 c)
	    DataSet<Tuple3<String,String,Long>>flightsDelay =
	    		    flights.filter(new FilterFunction<Tuple4<String, String, String, String>>() {
	                            public boolean filter(Tuple4<String, String,String,String> entry){
	                            	try {
	                            	return (format.parse(entry.f2).getTime() < format.parse(entry.f3).getTime());}  // filter only delayed flights
	                            	catch(ParseException e) {
	                            		System.out.println("Ignore the cancelled flight");
	                            		return false;
	                            	}
	                            } 
	                     }).flatMap(new TimeDifferenceMapper());
    
	 //Step 2)    
	    DataSet<Tuple2<String, String>> surtedUSairlines = USairlines.sortPartition(1,Order.ASCENDING);  //  1 is airline name
		
	    
	// Step 3	
		DataSet<Tuple2<String, Long>> flightsCraftes = aircraftsYear
			  .join(flightsDelay).where(0).equalTo(1).projectSecond(0,2);  // carrier code , number of delay 	
		    
	    DataSet<Tuple2<String,Long>> joinresult = USairlines
		      .join(flightsCraftes).where(0).equalTo(0).projectFirst(1).projectSecond(1); // airline name, number of delay
	    
	    
	    
	// Step 4
	    DataSet<Tuple2<String,Integer>> joinresult_num = joinresult.flatMap(new NumMapper())
	    	      .groupBy(0) 
	    	      .sum(1);  
	    
	    DataSet<Tuple3<String,Integer,Long>> joinresult_num_sum = joinresult.groupBy(0).sum(1)
	    		.join(joinresult_num).where(0).equalTo(0).projectSecond(0,1).projectFirst(1);
	    		
	    DataSet<Tuple4<String,Integer,Long,Long>> joinresult_num_sum_min = joinresult.groupBy(0).min(1)
	    		.join(joinresult_num_sum).where(0).equalTo(0).projectSecond(0,1,2).projectFirst(1);
	    
	    DataSet<Tuple5<String,Integer,Long,Long,Long>> joinresult_num_sum_min_max = joinresult.groupBy(0).max(1)
	    		.join(joinresult_num_sum_min).where(0).equalTo(0).projectSecond(0,1,2,3).projectFirst(1);
	    
	    
    // Step 5
	    DataSet<Tuple5<String,Integer,Long,Long,Long>> finalresult = 
	    		joinresult_num_sum_min_max.flatMap(new AvgMapper());
	    		//.sortPartition(0,Order.ASCENDING);
	    
   
	    //write out final result
	    finalresult.writeAsText(outputFilePath, WriteMode.OVERWRITE);   
        // execute the FLink job
	    env.execute("Executing task 2 program");
	    
	  
	    // wait 20secs at end to give us time to inspect ApplicationMAster's WebGUI
	    Thread.sleep(40000); 
	    
	}
	/**
	* Calculate delay time for each delay flight
	* After get the delay time, filter out the scheduled and actual departure time columns
	* View step 1 c)
	*/
	 private static class TimeDifferenceMapper implements FlatMapFunction<Tuple4<String, String, String, String>, Tuple3<String,String,Long>>{
	     SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
	          @Override
	          public void flatMap(Tuple4<String, String, String, String>input_tuple, Collector<Tuple3<String,String,Long>> out) throws ParseException { 
	        	  Long diff_min =(long) ((format.parse(input_tuple.f3).getTime()-format.parse(input_tuple.f2).getTime())/(60.0 * 1000.0) % 60.0);
	        	  out.collect(new Tuple3<String,String,Long>(input_tuple.f0, input_tuple.f1, diff_min)); 
		    }
		  }
	/**
	* Count the number of delay flights for each airline
	* View step 3
	*/	   
	  private static class NumMapper implements FlatMapFunction<Tuple2<String,Long>, Tuple2<String,Integer>> {
		    @Override
		    public void flatMap( Tuple2<String,Long> input_tuple, Collector<Tuple2<String,Integer>> out) {
		      out.collect(new Tuple2<String,Integer>(input_tuple.f0,1));
		    }
		  }
	/**
	* Calculate average delay time based on the count
	* View step 3
	*/
	  private static class AvgMapper implements FlatMapFunction<Tuple5<String,Integer,Long,Long,Long>, Tuple5<String,Integer,Long,Long,Long>> {
		    @Override
		    public void flatMap(Tuple5<String,Integer,Long,Long,Long> input_tuple, Collector<Tuple5<String,Integer,Long,Long,Long>> out) {
		    	Long avg = input_tuple.f2/input_tuple.f1;
		      out.collect(new Tuple5<String,Integer,Long,Long,Long>(input_tuple.f0,input_tuple.f1,avg,input_tuple.f3,input_tuple.f4));
		    }
		  }
		  
	 

}
