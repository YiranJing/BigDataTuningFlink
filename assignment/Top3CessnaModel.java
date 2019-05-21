package assignment;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.log4j.BasicConfigurator;


import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


public class Top3CessnaModel {
	  public static void main(String[] args) throws Exception {
		  
			BasicConfigurator.configure();
			
			/****************************
			*** READ IN DATA NEEDED. ***
			****************************/
			
			
			final String PATH = "/Users/yiranjing/Desktop/DATA3404/assignment_data_files/";
			final ParameterTool params = ParameterTool.fromArgs(args);
			String outputFilePath = params.get("output", PATH + "results/bad_top_3_cessna_medium.txt");
    
		    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		    DataSet<Tuple3<String,String, String>> models =
		      env.readCsvFile(PATH + "ontimeperformance_aircrafts.csv")
		      .includeFields("10101") //tail_number, manufacture, model
		      .ignoreFirstLine() 
		      .ignoreInvalidLines() 
		      .types(String.class, String.class,String.class); 
		    
		    DataSet<Tuple1<String>> flights =
				      env.readCsvFile(PATH+"ontimeperformance_flights_medium.csv")
				      .includeFields("0000001") // tail_number
				      .ignoreInvalidLines() 
				      .types(String.class); 
		    
		
		    /****************************
			*** ACTUAL IMPLEMENTATION ***
			****************************/						

			/****************************
			*	Implementation
			*
			* 1) Join the data sets
			* 2) Filter CESSNA Model
			* 3) Group by each model and count how many flights per model,Pick top 3
			****************************/

		    // Step 1
		    DataSet<Tuple4<String, String, String, String>> joinresult =
		       flights.join(models).where(0).equalTo(0)
		       .projectFirst(0)
		       .projectSecond(0,1,2);
		     
		    
		    // Step 2
		    DataSet<Tuple1<String>> modelsCessna =
		    		joinresult.filter(new FilterFunction<Tuple4<String, String,String, String>>() {
				                            public boolean filter(Tuple4<String, String,String, String> entry) { return entry.f2.equals("CESSNA"); } 
				       })
		    		.project(3); //manufacture and model;
		    
		    // step 3
		    modelsCessna
		    		.flatMap(new CountFlightPerModel())
		    		.groupBy(0)  // model
		    		.sum(1)          
				    .sortPartition(1, Order.DESCENDING) // number of flights in decreasing order
				    .setParallelism(1)
				    .first(3) // pick only top 3
				    .writeAsText(outputFilePath, WriteMode.OVERWRITE);
				   
		   

		    // execute the FLink job
		   env.execute("Executing task 1 medium bad version");
		   
		   // option 2
		   //System.out.println(env.getExecutionPlan());
		  
		  }
	  
	/**
	* Count how many flights per model
	* View step 3
	*/
	  private static class CountFlightPerModel implements FlatMapFunction<Tuple1<String>, Tuple2<String,Integer>> {
	    @Override
	    public void flatMap( Tuple1<String> input_tuple, Collector<Tuple2<String,Integer>> out) {
	    
	    	input_tuple.f0 = input_tuple.f0.replaceAll("[^0-9]+", " ").trim().substring(0,3); // delete non-digits and then keep only first 3 digits
	    	input_tuple.f0 ="Cessna "+input_tuple.f0;
	    	out.collect(new Tuple2<String,Integer>(input_tuple.f0,1));
	    }
	  }
	  
	  
}
