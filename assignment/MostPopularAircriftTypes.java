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

public class MostPopularAircriftTypes {
	
	public static void main(String[] args) throws Exception {
			  
			BasicConfigurator.configure();
			
			String  = args[0];  // command argument, in this code, I use United States temporarily
			 
			// get output file command line parameter - or use "top_rated_users.txt" as default
		    final ParameterTool params = ParameterTool.fromArgs(args);
		    String output_filepath = params.get("output", "/Users/yiranjing/Desktop/DATA3404/assignment_data_files/results/most_popular.txt");

		    
		    // obtain handle to execution environment
		    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		    
		    DataSet<Tuple2<String, String>> flights =
				      env.readCsvFile("/Users/yiranjing/Desktop/DATA3404/assignment_data_files/ontimeperformance_flights_tiny.csv")
				      .includeFields("0100001") // the second column is airlines code, 7-th col is tail_number
				      .ignoreFirstLine() // csv has header
				      .ignoreInvalidLines() // need it
				      .types(String.class,String.class); // these two columns are integer type
		    
		     // load the second dataset
			    DataSet<Tuple3<String,String, String>> models =
			      env.readCsvFile("/Users/yiranjing/Desktop/DATA3404/assignment_data_files/ontimeperformance_aircrafts.csv")
			      .includeFields("10101") // 1-th tailernumber, 3 is manufacturer, 5-th is model
			      .ignoreFirstLine() // csv has header
			      .ignoreInvalidLines() // need it
			      .types(String.class, String.class, String.class); 
			       
		    // load the third dataset
		    DataSet<Tuple3<String, String, String>> airlines =
				      env.readCsvFile("/Users/yiranjing/Desktop/DATA3404/assignment_data_files/ontimeperformance_airlines.csv")
				      .includeFields("111") 
				      .ignoreFirstLine() // csv has header
				      .ignoreInvalidLines() // need it
				      .types(String.class,String.class,String.class); // these two columns are integer type
		    
		    
		    // Filter dataset for\ specific airline
		    DataSet<Tuple3<String, String, String>> airlinesCountry =
		    		airlines.filter(new FilterFunction<Tuple3<String, String, String>() {
		                            public boolean filter(Tuple3<String, String, String> entry) { return entry.f2.equals("United States"); } 
		            }); 
            
		    // keep smaller relation as outer
		    DataSet<Tuple2<String, String >> FlightCountry = airlinesCountry
				      .join(flights)
				      .where(0) // key of the first relation (tuple field 0)           
				      .equalTo(1) // key of the second relation (tuple field 0)
				      .projectFirst(1) // name of airline
				      .projectSecond(1); // tail_number
		    
		    
			 // Equal Join (JoinHint.BROADCAST_HASH_FIRST maybe )
			 // join the two datasets
			 // keep smaller relation as outer(if local join)
		    DataSet<Tuple3<String, String, String>> joinresult = models
			      .join(airlinesCountry)
			      .where(0) // tailernumber of outer          
			      .equalTo(1) 
			      .projectFirst(1,2)  // manufacturer and model 
			      .projectSecond(1); // airline name 
			      //.groupBy(1,2);

		    
		    
		    
		    
		    /*
		    //write out final result
		    finalresult.writeAsText(output_filepath, WriteMode.OVERWRITE);
		    		
		    		
		    
		    // execute the FLink job
		    env.execute("Executing task 1 program");
		    // alternatively: get execution plan
		    
		    //System.out.println(env.getExecutionPlan());

		    // wait 20secs at end to give us time to inspect ApplicationMAster's WebGUI
		    //Thread.sleep(20000); 
		     * 
		     */
		    
		}

}
