package assignment;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.log4j.BasicConfigurator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
			String outputFilePath = params.get("output", PATH + "results/top_3_cessna.txt");
    
		    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		    DataSet<Tuple3<String,String, String>> models =
		      env.readCsvFile(PATH + "ontimeperformance_aircrafts.csv")
		      .includeFields("10101") 
		      .ignoreFirstLine() 
		      .ignoreInvalidLines() 
		      .types(String.class, String.class,String.class); 
		    
		    DataSet<Tuple1<String>> flights =
				      env.readCsvFile(PATH+"ontimeperformance_flights_tiny.csv")
				      .includeFields("0000001") 
				      .ignoreFirstLine() 
				      .ignoreInvalidLines() 
				      .types(String.class); 
		    
		
		    /****************************
			*** ACTUAL IMPLEMENTATION ***
			****************************/						

			/****************************
			*	Implementation
			*
			* 1) Filter CESSNA Model
			* 2) Join the filtered data sets 
			* 3) Group by each model and count how many flights per model, and pick top 3 only after sort 
			****************************/
		    
		    
		    
	    // Step 1
		    DataSet<Tuple2<String, String>> modelsCessna =
		    		models.filter(new FilterFunction<Tuple3<String,String,String>>() {
		                            public boolean filter(Tuple3<String, String, String> entry) { return entry.f1.equals("CESSNA"); } // f1 is second field.
		            }).project(0,2);  
		    
	    // Step 2 
		    DataSet<Tuple1<String>> joinresult =
		      modelsCessna.join(flights).where(0).equalTo(0).projectFirst(1);   
		    
	    // Step 3
		    joinresult.flatMap(new CountFlightPerModel())
		      .groupBy(0) 
		      .sum(1)          
		      .sortPartition(1, Order.DESCENDING) // number of flights in decreasing order
		      .first(3) // pick only top 3
		      .writeAsText(outputFilePath, WriteMode.OVERWRITE);
		   

		    // execute the FLink job
		    env.execute("Executing task 1 program");
		   
		   // Thread.sleep(20000);
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
