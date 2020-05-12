package java_project;

/**Importing Libraries**/
import java.io.IOException;

/**********************************************************************************
* Implementing a testDf class
* We will use this file to test all of the functions of MyDataFrame
**********************************************************************************/

public class testDf {

	public static void main(String[] args) throws IOException {
	
		
		/**
		 * Create a MyPandas Object
		 */
		//Path of the file we will be using to test the functions 
		String pathname = "babynames.csv";	
		System.out.println("*******************************************");
		System.out.println("Create a MyPandas Object");
		//Create a MyPandas Object
		MyPandas babynames = new MyPandas(pathname);
		System.out.println("MyPandas df created"); 
		
		/**
		 * readCSV
		 */
		//Reading in data from .csv into MyDataFrame
		System.out.println("*******************************************");
		System.out.println("Reading in data from .csv into MyDataFrame");
		MyDataFrame data = babynames.readCSV(pathname); 
		data.print();
		
		/**
		 * Sort
		 */
		//Sorted Data
		System.out.println("*******************************************");
		System.out.println("Sorting MyDataFrame by 'State'");
		MyDataFrame sort_data = data.sort("state");
		sort_data.print();
		
		System.out.println("*******************************************");
		System.out.println("Sorting MyDataFrame by column index 4");
		MyDataFrame sort_data_ind = data.sort(4);
		sort_data_ind.print();
		
		
		/**
		 * Head
		 * Tail
		 */
		//Head
		System.out.println("*******************************************");
		System.out.println("Head");
		MyDataFrame head = data.head(5);
		head.print();
		//Tail
		System.out.println("*******************************************");
		System.out.println("Tail");
		MyDataFrame tail = data.tail(5);
		tail.print();

		/**
		 * Concat
		 * writeCSV
		 */
		//concat
		System.out.println("*******************************************");
		System.out.println("DataFrame Concatenation");
		System.out.println("Concatenate the previous head and tail data frames into a combined data frame"); 
		MyDataFrame data_concat = babynames.concat(head, tail);
		data_concat.print();
		//writeCSV
		babynames.writeCSV(data_concat, "writeConcatFile.csv");
		
		/**
		 * Slice:
		 * By integer array
		 * By string array
		 * By integer
		 * By string
		 */
		//By integer array
		System.out.println("*******************************************");
		System.out.println("Slice by integer array {0,2,3}"); 
		int[] intArray = new int[] {0,2,3};
		MyDataFrame slice = data.slice(intArray);
		slice.print();
		//By string array 
		System.out.println("*******************************************");
		System.out.println("Slice by string array {state, gender}"); 
		String[] strArray = new String[] {"state","gender"};
		MyDataFrame slice_1 = data.slice(strArray);
		slice_1.print();
		//By integer
		System.out.println("*******************************************");
		System.out.println("Slice by index 0"); 
		MyDataFrame slice_2 = data.slice(0);
		slice_2.print(); 
		//By string
		System.out.println("*******************************************");
		System.out.println("Slice by label 'state'"); 
		MyDataFrame slice_3 = data.slice("state");
		slice_3.print(); 

		
		/** 
		 * Indexing: 
		 * By integer
		 * By string
		 * From integer to integer
		 * From string to string 
		 */
		//By integer 
		System.out.println("*******************************************");
		System.out.println("Indexing by integer - 3"); 
		MyDataFrame loc_1 = data.loc(3);
		loc_1.print();
		//By string 
		System.out.println("*******************************************");
		System.out.println("Indexing by string - '3'"); 
		MyDataFrame loc_2 = data.loc("3");
		loc_2.print();
		//From integer to integer
		System.out.println("*******************************************");
		System.out.println("Indexing from integer to integer - 2 to 6"); 
		MyDataFrame loc_3 = data.loc(2,6);
		loc_3.print();
		//From string to string 
		System.out.println("*******************************************");
		System.out.println("Indexing from string to string - '2' to '6'"); 
		MyDataFrame loc_4 = data.loc("2","6");
		loc_4.print();

		
		/**
		 * Filtering 
		 */
		//Filtering by a string column
		System.out.println("*******************************************");
		System.out.println("Filtering by string column : state != 'FL'"); 
		MyDataFrame filter = data.filter("state", "!=", "FL");
		filter.print();
		//Filtering by an integer column 
		System.out.println("*******************************************");
		System.out.println("Filtering by an integer column : year <= 2018");
		MyDataFrame filter_n = data.filter("year", "<=", "2018");
		filter_n.print();
		
		/**
		 * Dtype 
		 * By Index
		 * By String 
		 */
		//Dtype by an index
		System.out.println("*******************************************");
		System.out.println("Dtype by index - 0"); 
		String dtype1 = data.dType(0);
		System.out.println(dtype1); 
		//Dtype by a string 
		System.out.println("*******************************************");
		System.out.println("Dtype by label - 'year'"); 
		String dtype2 = data.dType("year");
		System.out.println(dtype2); 
		
		
		/**
		 * getMin and getMax interpretation: Grab the smallest and largest element from a columm 
		 * getMin of a column by integer
		 * getMin of column by string
		 * getMax of column by integer
		 * getMax of column by string 
		 */
		//getMin by integer
		System.out.println("*******************************************");
		System.out.println("getMin by index - 0"); 
		Object min_int = data.getMin(0);
		System.out.println(min_int); 
		//getMin by string 
		System.out.println("*******************************************");
		System.out.println("getMin by label - year"); 
		Object min_string = data.getMin("year");
		System.out.println(min_string);
		//getMax by integer
		System.out.println("*******************************************");
		System.out.println("getMax by index - 0"); 
		Object max_int = data.getMax(0);
		System.out.println(max_int); 
		//getMax by string 
		System.out.println("*******************************************");
		System.out.println("getMax by label - year"); 
		Object max_string = data.getMax("year");
		System.out.println(max_string);
		
		/**
		 * Aggregation interpretation of getMin and getMax: return the most and least frequent value of a column 
		 * * getMin of a column by integer
		 * getMin of column by string
		 * getMax of column by integer
		 * getMax of column by string 
		 */
		//getMin_agg integer
		System.out.println("*******************************************");
		System.out.println("getMin_agg Least frequent value of column index - 0"); 
		Object min_agg_int = data.getMin_agg(0); 
		System.out.println(min_agg_int);
		//getMin agg string 
		System.out.println("*******************************************");
		System.out.println("getMin_agg Least frequent value of column label - state"); 
		Object min_agg_string = data.getMin_agg("state"); 
		System.out.println(min_agg_string);
		//getMax agg integer
		System.out.println("*******************************************");
		System.out.println("getMax_agg Most frequent value of column index - 0"); 
		Object max_agg_int = data.getMax_agg(0); 
		System.out.println(max_agg_int);
		//getMax agg string 
		System.out.println("*******************************************");
		System.out.println("getMax_agg Most frequent value of column label - state"); 
		Object max_agg_string = data.getMax_agg("state"); 
		System.out.println(max_agg_string); 


	}

}
