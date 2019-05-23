import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.log4j.BasicConfigurator;

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
		final String PATH = "/Users/jazlynj.m.lin/assignment_data_files/";
		//final String PATH = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/";
		final ParameterTool params = ParameterTool.fromArgs(args);
		String outputFilePath = params.get("output", PATH + "results/most_popular_result_tiny.txt");
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
        *	1) flights join aircrafts join airlines
        *   2) Apply filter for United States
		*   3) Rank grouped by aircraft types
		****************************/

        // Step 1: Join datasets.

        // Join to get <airlines.tail_number, flights.name> based on carrier code.
        // Join on carrier code.
        // Input: (carrier code, name, country) X (carrier code, tail number)
        // Output: (name, country) X (tail number)
        DataSet<Tuple3<String, String, String>> flightsOnAirlines = 
            airlines.join(flights)
            .where(0)
            .equalTo(0)
            .with(new EquiJoinFlightsWithAirlines());


        // Join to get <flightsOnAirlines.tail_number, flightsOnAirlines.name, aircrafts.model, aircrafts.year>
        // Join on tail number.
        // Input: [(name, country) X (tail number)] X (tail_number, manufacturer, model)
        // Output: [(name, country) X (tail number)] X (manufacturer, model)
        Dataset<Tuple5<String, String, String, String, String>> finalData =
            flightsOnAirlines.join(aircrafts)
            .where(2)
            .equalTo(0)
            .with(new EquiJoinflightsOnAirlinesWithAircrafts());

        
        // Step 2: Reduce dataset and generate ordered list by airline and tailnumber.
        // 2.1: Group data by tailNumber
        // 2.2: Count unique tailNumbers
        // 2.3: Sort by airline name and tailNumber count.

        // <airline_name, tail_number, manufacturer, model, count>
        // Input: [(name) X (tail number)] X (manufacturer, model)
        // Output: [(name) X (tail number)] X (manufacturer, model) X (Count)
        DataSet<Tuple6<String, String, String, String, String, Integer>> aircraftCount =
              finalData.groupBy(2) // Tail number
              .reduceGroup(new Rank())
              .sortPartition(0, Order.ASCENDING)
              .sortPartition(5, Order.DESCENDING);


        // Step 3: Apply reduction so that only the 5 most used tailnumbers for each airline is recorded.
        // Input: [(name) X (tail number)] X (manufacturer, model)
        // Output: (airlines.name, [aircrafts.manufacturer, aircrafts.model])
        DataSet<Tuple3<String, String, ArrayList<Tuple2<String, String>>>> finalResult = 
              aircraftCount.reduceGroup(new RetrieveTopFive());
        
        
        // Step 4: Get US airlines.
        finalResult.filter(new FilterFunction<Tuple3<String, String, ArrayList<Tuple2<String, String>>>>() {
            @Override
            public boolean filter(Tuple3<String, String, String> tuple) {
                // Filter for user specified country.
                return tuple.f1.contains("United States"); }
            })
            .project(0, 2);

        
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

  /** View step 2
	* Equi-join filtered airlines with flights on carrier code.
	* Return: <airlines.name, airlines.country flights.tail_number>
	*/
	public static class EquiJoinFlightsWithAirlines implements JoinFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple3<String, String, String>> {
		@Override
		public Tuple3<String, String, String, String> join(Tuple3<String, String, String> airlines, Tuple3<String, String, String> flights){
			return new Tuple3<>(airlines.f1, airlines.f2, flights.f2);
		}
    }    

  /** View step 2
	* Equi-join flightsWithAirlines with aircraft on tail number.
	* Return: <flightsOnAirlines.tail_number, flightsOnAirlines.name, aircrafts.model, aircrafts.year>
	*/
	public static class EquiJoinflightsOnAirlinesWithAircrafts implements JoinFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple5<String, String, String, String, String>> {
		@Override
        public Tuple5<String, String, String, String, String> join(Tuple3<String,String, String> flightsOnAirlines, Tuple3<String, String, String> aircrafts)
        {
            // Alternatively can replace aircrafts.f0 with flightsOnAirlines.f1
            return new Tuple4<String, String, String, String>(flightsOnAirlines.f0,flightsOnAirlines.f1, aircrafts.f0, aircrafts.f1, aircrafts.f2);
        }
    }    

  /** View Step 3
    * Pipeline result to store count of model.
    * Return: [(name) X (tail number)] X (manufacturer, model) X (Count)
    */
    public static class Rank implements GroupReduceFunction<Tuple5<String,String, String, String, String>, Tuple6<String,String, String, String, String, Integer>> {
        @Override
        public void reduce(Iterable<Tuple5<String,String, String, String, String>> combinedData, Collector<Tuple6<String, String, String,String, String, Integer>> outputTuple) throws Exception {
            // To keep track of values for each tail number.
            String manufacturerEntry,modelEntry,tailNumber,airlineName,countryName;
            manufacturerEntry = modelEntry = tailNumber = airlineName = countryName = null;

            int modelCount = 0;
            for (Tuple5<String, String, String, String, String> entry : combinedData)
            {
                // Iterate through and count for each tail number.
                count++;
                airlineName = entry.f0;
                countrName = entry.f1;
                tailNumber = entry.f2;
                manufacturerEntry = entry.f3;
                modelEntry = entry.f4;
            }
            outputTuple.collect(new Tuple6<String,String, String, String, String, Integer>(airlineName, countryName, tailNumber, manufacturerEntry, modelEntry, modelCount));
        }
    }

  /** View Step 4
    * Pipeline result to store count of model.
    * Return: (airlines.name, [aircrafts.manufacturer, aircrafts.model])
    */
    public static class RetrieveTopFive implements GroupReduceFunction<Tuple6<String, String, String, String, String, Integer>, Tuple3<String, String, ArrayList<Tuple2 <String, String>>>> {
        @Override
        public void reduce(Iterable<Tuple6<String, String, String, String, String, Integer>> entries, Collector<Tuple3<String, String, ArrayList<Tuple2 <String, String>>>> output) throws Exception {
            int limitCount = 0;
            ArrayList<String> modelMostUsed = new ArrayList<String>();
            String currentAirline, countryName;
            currentAirline = countryName = "";
            ArrayList<Tuple2<String, String>> mostUsedList = new ArrayList<Tuple2<String, String>>();
            
            for (Tuple6<String, String, String, String, String, Integer> entry : records) {
                countryName = entry.f1;
                if (currentAirline.equals(""))
                {
                    // For first row.
                    currentAirline = entry.f0;
                } 
                if (!(entry.f0.equals(currentAirline)) || limitCount == 5)
                {
                    if (entry.f0.equals(currentAirline))
                    {
                        // We just used up the max count so continue on.
                        continue;
                    }
                    // New entry name so need to output result and update current airline.
                    output.collect(new Tuple2<String, ArrayList<Tuple2<String, String>>>(currentAirline, countryName, mostUsedList));
                    
                    // Now we are on new airline name.
                    modelMostUsed.clear();
                    mostUsedList.clear();
                    currentAirline = entry.f0;
                    limitCount = 0;
                }
                if (modelMostUsed.contains(entry.f4))
                {
                    // Model has been seen before. Continue.
                    continue;
                } 

                // New model so let's keep track of it.
                modelMostUsed.add(entry.f4);
                mostUsedList.add(new Tuple2<String, String>(entry.f3, entry.f4));
                limitCount++;
            }
            // Don't forget about the last one!
            output.collect(new Tuple3<String, String, ArrayList<Tuple2<String, String>>>(currentAirline, countryName, mostUsedList));
        }
    }

  /** View Step 5
    * Write results out to file.
    * Collate results for output.
    */
	private static class OutputResults implements GroupReduceFunction<DataSet<Tuple2<String, ArrayList<Tuple2<String, String>>>>, Tuple2 <String, String> > {
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