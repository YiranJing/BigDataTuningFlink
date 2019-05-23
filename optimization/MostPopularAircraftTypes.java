package optimization;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.log4j.BasicConfigurator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Scanner;
import java.io.BufferedWriter;
import java.io.FileWriter;
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

public class MostPopularAircraftTypes {
	
	public static void main(String[] args) throws Exception {

		BasicConfigurator.configure();
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/****************************
		*** READ IN DATA NEEDED. ***
		****************************/
		Scanner scanner = new Scanner(System.in);
        System.out.println("Enter a country:" );  
		String country = scanner.nextLine();
		scanner.close();
		
		// Don't forget to change file path!
		final String PATH = "/Users/yiranjing/Desktop/DATA3404/assignment_data_files/";
		//final String PATH = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/";
		final ParameterTool params = ParameterTool.fromArgs(args);
		String outputFilePath = params.get("output", PATH + "results/optimize_most_popular_result_tiny.txt");
		//String outputFilePath = params.get("output", PATH + "user/jlin0701/assignment_data_files/results/most_popular_result_tiny.txt");
		
		// (carrier code, tail number)
		DataSet<Tuple2<String, String>> flights =
		env.readCsvFile(PATH + "ontimeperformance_flights_tiny.csv")
		//env.readCsvFile(PATH + "share/data3404/assignment/ontimeperformance_flights_tiny.csv")
						.includeFields("010000100000")
						.ignoreFirstLine()
						.ignoreInvalidLines()
						.types(String.class, String.class);
		
		// (carrier code, name, country)
		DataSet<Tuple3<String, String, String>> airlines =
		env.readCsvFile(PATH + "ontimeperformance_airlines.csv")
		//env.readCsvFile(PATH + "share/data3404/assignment/ontimeperformance_airlines.csv")
						.includeFields("111")
						.ignoreFirstLine()
						.ignoreInvalidLines()
						.types(String.class, String.class, String.class);
		
		// (tail_number, manufacturer, model)
		DataSet<Tuple3<String, String, String>> aircrafts =
		env.readCsvFile(PATH + "ontimeperformance_aircrafts.csv")
		//env.readCsvFile(PATH + "share/data3404/assignment/ontimeperformance_aircrafts.csv")
						.includeFields("10101")
						.ignoreFirstLine()
						.ignoreInvalidLines()
						.types(String.class, String.class, String.class);

		/****************************
		*** ACTUAL IMPLEMENTATION ***
		****************************/						

		/****************************
        *	Implementation
        *   1) Apply filter for United States
		*	2) flights join aircrafts join airlines
		*   3) Rank grouped by aircraft types
		****************************/

    
        // Step 1: Filter and retrieve and return carrier code  + name based off Country.
        // Input: (carrier code, name, country)
        // Output: (carrier code, name)
        airlines.filter(new FilterFunction<Tuple3<String, String, String>>() {
        @Override
        public boolean filter(Tuple3<String, String, String> tuple) {
            // Filter for user specified country.
            return tuple.f2.contains("United States"); }   // will change to country
        })
        .project(0, 1);
        
        // Step 2: Join datasets.

        // Join on carrier code.
        // Input: (carrier code, name) X (carrier code, tail number)
        // Output: (name) X (tail number)
        DataSet<Tuple2<String, String>> flightsOnAirlines = 
            airlines.join(flights, JoinHint.BROADCAST_HASH_FIRST)
            .where(0)
            .equalTo(0)
            .projectFirst(1)
            .projectSecond(1);
            

        // Join on tail number.
        // Input: [(name, tail number)] X (tail_number, manufacturer, model)
        // Output: [(name) X (tail number)] X (manufacturer, model)
        DataSet<Tuple4<String, String, String, String>> finalData =
            flightsOnAirlines.join(aircrafts, JoinHint.BROADCAST_HASH_FIRST)
            .where(1)
            .equalTo(0)
            .projectFirst(0,1)
            .projectSecond(1,2);

        
        // Step 3: Reduce dataset and generate ordered list by airline and tailnumber.
        // 3.1: Group data by tailNumber
        // 3.2: Count unique tailNumbers
        // 3.3: Sort by airline name and tailNumber count.

        // Input: [(name, tail number, manufacturer, model)
        // Output: [(name) X (tail number)] X (manufacturer, model) X (Count)
        DataSet<Tuple5<String, String, String, String, Integer>> aircraftCount =
              finalData.groupBy(1)
              .reduceGroup(new Rank())
              .sortPartition(0, Order.ASCENDING)
              .sortPartition(4, Order.DESCENDING);


        // Step 4: Apply reduction so that only the 5 most used tailnumbers for each airline is recorded.
        // Input: [(name) X (tail number)] X (manufacturer, model) X (Count)
        // Output: (airlines.name, [aircrafts.manufacturer, aircrafts.model])
        DataSet<Tuple2<String, ArrayList<Tuple2<String, String>>>> finalResult = 
              aircraftCount.reduceGroup(new RetrieveTopFive());
        
        
        // Step 5: Retrieve and write out results from step 4 into file.
        // Input: (airlines.name, [aircrafts.manufacturer, aircrafts.model])
        // Output: (airlines.name, [aircraft_type1, ..., aircraft_type5])
        DataSet<Tuple2<String, String>> outputResult = finalResult
        .reduceGroup(new OutputResults())
        .sortPartition(0, Order.ASCENDING);
        
        outputResult.writeAsText(outputFilePath, WriteMode.OVERWRITE);
    
		}

    /****************************
    *** HELPER FUNCTIONS.    ****
    ****************************/


  /** View Step 3
    * Pipeline result to store count of model.
    * Return: [(name) X (tail number)] X (manufacturer, model) X (Count)
    */
    public static class Rank implements GroupReduceFunction<Tuple4<String, String, String, String>, Tuple5<String, String, String, String, Integer>> {
        @Override
        public void reduce(Iterable<Tuple4<String, String, String, String>> combinedData, Collector<Tuple5<String, String, String, String, Integer>> outputTuple) throws Exception {
            String manufacturerEntry,modelEntry,tailNumber,airlineName;
            manufacturerEntry = modelEntry = tailNumber = airlineName = null;

            int modelCount = 0;
            for (Tuple4<String, String, String, String> entry : combinedData) {
            	modelCount++;
                airlineName = entry.f0;
                tailNumber = entry.f1;
                manufacturerEntry = entry.f2;
                modelEntry = entry.f3;
            }
            outputTuple.collect(new Tuple5<String, String, String, String, Integer>(airlineName, tailNumber, manufacturerEntry, modelEntry, modelCount));
        }
    }


  /** View Step 4
    * Pipeline result to store count of model.
    * Return: (airlines.name, [aircrafts.manufacturer, aircrafts.model])
    */
    public static class RetrieveTopFive implements GroupReduceFunction<Tuple5<String, String, String, String, Integer>, Tuple2<String, ArrayList<Tuple2 <String, String>>>> {
        @Override
        public void reduce(Iterable<Tuple5<String, String, String, String, Integer>> entries, Collector<Tuple2<String, ArrayList<Tuple2 <String, String>>>> output) throws Exception {
            int limitCount = 0;
            ArrayList<String> modelList = new ArrayList<String>();
            String currentAirline = "";
            ArrayList<Tuple2<String, String>> aircraftType = new ArrayList<Tuple2<String, String>>();

            // Iterate through list of records.
            // If we stumble upon a new airline name that is different to our current airline or we reached the limit of 5 models per airline
            // We save the current results for our current airline and update our current variables to keep note of next airline results.
            for (Tuple5<String, String, String, String, Integer> entry : entries)
            {
                if (currentAirline.equals(""))
                {
                    // For first row.
                    currentAirline = entry.f0;
                } 

                // Hit our limit of 5.
                if (!(entry.f0.equals(currentAirline)) || limitCount == 5)
                {

                    if(entry.f0.equals(currentAirline))
                    {
                        // We just keep moving on.
                        continue;
                    }
                    else
                    {    
                        // New airline so output current data.
                        output.collect(new Tuple2<String, ArrayList<Tuple2<String, String>>>(currentAirline, aircraftType));
                        // Now we are on new airline name.
                        modelList.clear();
                        aircraftType.clear();
                        currentAirline = entry.f0;
                        limitCount = 0;
                    }
                }

                if (modelList.contains(entry.f3))
                {
                    // Model has been seen before. Continue.
                    continue;
                } 

                if (modelList.contains(entry.f3))
                {
                    continue;
                }
                // New model so let's keep track of it.
                modelList.add(entry.f3);
                aircraftType.add(new Tuple2<String, String>(entry.f2, entry.f3));
                limitCount++;
                continue;     
            }
            // Don't forget about the last one!
            output.collect(new Tuple2<String, ArrayList<Tuple2<String, String>>>(currentAirline, aircraftType));
        }
    }

  /** View Step 5
    * Write results out to file.
    * Collate results for output.
    */
	private static class OutputResults implements GroupReduceFunction<Tuple2<String, ArrayList<Tuple2<String, String>>>, Tuple2<String, String> > {
		@Override
		public void reduce(Iterable<Tuple2<String, ArrayList<Tuple2<String, String>>>> object, Collector<Tuple2<String, String>> output) throws Exception {
			
			String head = null; 
			String line = null;
			for(Tuple2<String,  ArrayList<Tuple2<String, String>>> count : object){
				if(head == null){
					head = count.f0;
					line = "[";
			}
				if(count.f0.equals(head)){
					if(line.length() != 1){
						line += ", ";
                    }
                    // Construct string for manufacturer + ' ' + model.
                    line += count.f1.get(0) + " " + count.f1.get(1);
				}
			}
			line += "]";
			output.collect(new Tuple2<>(head, line));
		}
	}
}
