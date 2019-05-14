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
			
		    // get output file command line parameter - or use "top_rated_users.txt" as default
		    final ParameterTool params = ParameterTool.fromArgs(args);
		    String output_filepath = params.get("output", "/Users/yiranjing/Desktop/top_3_cessna.txt");
		    
		    // obtain handle to execution environment
		    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		    // load the first dataset
		    DataSet<Tuple3<String,String, String>> models =
		      env.readCsvFile("/Users/charleshyland/Dropbox/University-work/5th year/Semester 1/DATA3404/Assignment/assignment_data_files/ontimeperformance_aircrafts.csv")
		      .includeFields("10101") // the first column is join key, the third column is manufacturer, the fifth column is model
		      .ignoreFirstLine() // csv has header
		      .ignoreInvalidLines() // need it
		      .types(String.class, String.class,String.class); 
		    
		    
		    // data transformation
		    // do filter before  join
		    // filter for the specific manufacturer Cessna;
		    DataSet<Tuple2<String, String>> modelsCessna =
		    		models.filter(new FilterFunction<Tuple3<String,String,String>>() {
		                            public boolean filter(Tuple3<String, String, String> entry) { return entry.f1.equals("CESSNA"); } // f1 is second field.
		            }).project(0,2);  // keep only two columns: tail_number and  model. (they all belong to Cessna)
		    
		   
		    	    
		 // load the second dataset
		    DataSet<Tuple1<String>> flights =
		      env.readCsvFile("/Users/charleshyland/Dropbox/University-work/5th year/Semester 1/DATA3404/Assignment/assignment_data_files/ontimeperformance_flights_tiny.csv")
		      .includeFields("0000001") // only need the 7-th column (join key)
		      .ignoreFirstLine() // csv has header
		      .ignoreInvalidLines() // need it
		      .types(String.class); // these two columns are integer type
		   
		
		 
		 // Equal Join (JoinHint.BROADCAST_HASH_FIRST maybe )
		 // join the two datasets
		 // keep smaller relation as outer(if local join)
		    DataSet<Tuple1<String>> joinresult =
		      modelsCessna
		      .join(flights)
		      .where(0) // key of the first relation (tuple field 0)           
		      .equalTo(0) // key of the second relation (tuple field 1)
		      .projectFirst(1);   // keep only model number
		 
		 
		    
		// group by flight_id and count how many per model;
		    joinresult.flatMap(new CountFlightPerModel())
		      .groupBy(0) //group according to model
		      .sum(1)          
		      .sortPartition(1, Order.DESCENDING) // number of flights in decreasing order
		      .first(3) // pick only top 3
		      .writeAsText(output_filepath, WriteMode.OVERWRITE);
		    
		    

		    // execute the FLink job
		    env.execute("Executing task 1 program");
		    // alternatively: get execution plan
		    
		    //System.out.println(env.getExecutionPlan());

		    // wait 20secs at end to give us time to inspect ApplicationMAster's WebGUI
		   // Thread.sleep(20000);
		  }
	  
	 
	  private static class CountFlightPerModel implements FlatMapFunction<Tuple1<String>, Tuple2<String,Integer>> {
	    @Override
	    public void flatMap( Tuple1<String> input_tuple, Collector<Tuple2<String,Integer>> out) {
	    	input_tuple.f0 = input_tuple.f0.replaceAll("[^0-9]+", " ").trim();// delete non-digits
	    	input_tuple.f0 ="Cessna "+input_tuple.f0;
	    	//System.out.println(input_tuple.f0);
	    	out.collect(new Tuple2<String,Integer>(input_tuple.f0,1));
	    }
	  }
	  
	  
	  
}
