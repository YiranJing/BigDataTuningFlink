package assignment;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
		
		// get output file command line parameter - or use "top_rated_users.txt" as default
	    final ParameterTool params = ParameterTool.fromArgs(args);
	    String output_filepath = params.get("output", "/Users/yiranjing/Desktop/DATA3404/assignment_data_files/results/avg_dep_delay.txt");
	    
	    // obtain handle to execution environment
	    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	    
	    DataSet<Tuple4<String, String,String,String>> flights =
			      env.readCsvFile("/Users/yiranjing/Desktop/DATA3404/assignment_data_files/ontimeperformance_flights_tiny.csv")
			      .includeFields("0100001101") // the second column is airlines code, 8 and 10 are schedule and actual departure time
			      .ignoreFirstLine() // csv has header
			      .ignoreInvalidLines() // need it
			      .types(String.class,String.class,String.class,String.class); // these two columns are integer type
	    
	     // load the second dataset
		    DataSet<Tuple2<String,String>> models =
		      env.readCsvFile("/Users/yiranjing/Desktop/DATA3404/assignment_data_files/ontimeperformance_aircrafts.csv")
		      .includeFields("100000001") // the first column is join key, the 9-th column is year
		      .ignoreFirstLine() // csv has header
		      .ignoreInvalidLines() // need it
		      .types(String.class, String.class); 
		   
		    // Filter dataset based on specific year 
		    DataSet<Tuple2<String, String>> modelsYear =
		    		models.filter(new FilterFunction<Tuple2<String,String>>() {
		                            public boolean filter(Tuple2<String, String> entry) { return entry.f1.equals("2004"); } // f1 is second field.
		            }); 
		    
		    
		    

		    DataSet<Tuple3<String, String, String>> flightsYear = modelsYear
				      .join(flights)
				      .where(0) // key of the first relation (tuple field 0)           
				      .equalTo(1) // key of the second relation (tuple field 1)
				      .projectSecond(0,2,3); // keep the 		
		    
	    
	    // data transformation
	    // filter for delayed flight;
	    // to count how many ratings we have per user,
	   // we extend each entry witb a constant '1' which we later sum up over
	    SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
	     DataSet<Tuple2<String,Long>>flightsDelay =
	    		 flightsYear.filter(new FilterFunction<Tuple3<String, String, String>>() {
	                            public boolean filter(Tuple3<String, String,String> entry){
	                            	try {
	                            	return format.parse(entry.f1).getTime() <format.parse(entry.f2).getTime();}
	                            	catch(ParseException e) {
	                            		System.out.println("Ignore the cancelled flight");
	                            		return false;
	                            	}
	                            } 
	                     }).flatMap(new TimeDifferenceMapper());
	     
		    
		    
	     
	     //flightsDelay.writeAsText(output_filepath, WriteMode.OVERWRITE);
	    
	    // load the third dataset
	    DataSet<Tuple2<String, String>> airlines =
			      env.readCsvFile("/Users/yiranjing/Desktop/DATA3404/assignment_data_files/ontimeperformance_airlines.csv")
			      .includeFields("11") // Join key and the airline name
			      .ignoreFirstLine() // csv has header
			      .ignoreInvalidLines() // need it
			      .types(String.class,String.class); // these two columns are integer type
	    
		 // Equal Join (JoinHint.BROADCAST_HASH_FIRST maybe )
		 // join the two datasets
		 // keep smaller relation as outer(if local join)
	    DataSet<Tuple2<String,Long>> joinresult = airlines
		      .join(flightsDelay)
		      .where(0) // key of the first relation (tuple field 0)           
		      .equalTo(0) // key of the second relation (tuple field 0)
		      .projectFirst(1)  // keep only airline name
		      .projectSecond(1); // keep the delay time 
		 
	    
	    
	    /*
		// group by flight_id and count how many per model;
	    joinresult
	      .groupBy(0) //group according to model
	      .sum(1)          
	      //.sortPartition(0) // number of flights in decreasing order
	      .writeAsText(output_filepath, WriteMode.OVERWRITE); 
	    */
	    
	    joinresult.writeAsText(output_filepath, WriteMode.OVERWRITE);
	    
	    
	    
	    // execute the FLink job
	    env.execute("Executing task 1 program");
	    // alternatively: get execution plan
	    
	    //System.out.println(env.getExecutionPlan());

	    // wait 20secs at end to give us time to inspect ApplicationMAster's WebGUI
	    //Thread.sleep(20000);
	    
	    
	    
	}
	 
	 private static class TimeDifferenceMapper implements FlatMapFunction<Tuple3<String,String,String>, Tuple2<String,Long>>{
	  SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
		    public void flatMap(Tuple3<String,String,String>input_tuple, Collector<Tuple2<String,Long>> out) throws ParseException {
		      Long diff_min =(format.parse(input_tuple.f2).getTime()-format.parse(input_tuple.f1).getTime())/(60 * 1000) % 60;
		      out.collect(new Tuple2<String,Long>(input_tuple.f0, diff_min)); // get the same user_id only once.
		    }
		  }
	 

}
