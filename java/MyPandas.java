package java_project;

/**Importing Libraries**/
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**********************************************************************************
* Implementing a MyPandas class. The MyPandas class offers the following interfaces:

* 1.MyDataFrame readCSV(String path)
	Read a file and store it into a MyDataFrame object. 
	Possible data types: Integer and String. 
	The first row of the .csv file is assumed to be a header.

* 2.void writeCSV(MyDataFrame data, String path)
	Write a MyDataFrame object to file specified by path.

* 3.MyDataFrame concat(MyDataFrame df1, MyDataFrame df2)
	Concatenates two MyDataFrame objects along rows and returns the concatenated MyDataFrame.
**********************************************************************************/
 
public class MyPandas {
		
	
	public List<String> header; //stores the header
	public String path; //path to the .csv file
	MyDataFrame data_frame; 
	
	//The different columns are stored as Array Lists of Integers or Strings
    private ArrayList<String> state = new ArrayList<String>();
    private ArrayList<String> gender =new ArrayList<String>();  
    private ArrayList<Integer> year = new ArrayList<Integer>();
    private ArrayList<String> name = new ArrayList<String>();  
    private ArrayList<Integer> births = new ArrayList<Integer>(); 
    
    //Constructor
	public MyPandas(String path) {
		this.path = path;
	}
    
	/**
	 * Read a file and store it into a MyDataFrame object
	 * Possible data types: Integer and String
	 * The first row of the .csv file is assumed to be a header
	 
	 * @param path the path to the .csv file to be read in
	 * @return a MyDataFrame object
	 * @throws IOException
	 */
	public MyDataFrame readCSV(String path) throws IOException{
		File file = new File(path);
		
		try (Scanner scanner = new Scanner(file)) { //Scanning the file one line at a time
			int iteration = 0; //To read the first row as Header
			int n = 0; //To track the size of the data read in
			while (scanner.hasNextLine()) {
				if (iteration == 0) {
					header = Arrays.asList(scanner.nextLine().split(","));
					iteration ++;
				}
				else { //Appending corresponding values in appropriate column arrays
					String line = scanner.nextLine();
					state.add(line.split(",")[0]);
					gender.add(line.split(",")[1]);
					year.add(Integer.parseInt(line.split(",")[2]));
					name.add(line.split(",")[3]);
					births.add(Integer.parseInt(line.split(",")[4]));
					n++;
				}
			}
			
			//Creating a MyDataFrame from the column Arrays defined above
			data_frame = new MyDataFrame(n,header,state,gender,year,name,births);
		}
		catch (IOException e) { //If file not found
			System.out.println("File not found:\n");
		}
		
		return data_frame;
	}
	
	/**
	 * Writes a MyDataFrame object to a .csv file	
	 * @param data MyDataFrame object to be written
	 * @param path name of the .csv file within ""
	 * @throws IOException
	 */
	public void writeCSV(MyDataFrame data, String path) throws IOException{
		FileWriter writer = new FileWriter(path);
		List<String> heading = data.getHeader(); //Firsts writes the column names
		String head = heading.stream().collect(Collectors.joining(","));	    
	    writer.write(head + System.lineSeparator());

		for(int i = 0; i < data.getSize(); i++) { //Iterates through each row and writes to file
			
			List<String> row = new ArrayList<>();

			//Checks for the presence of said column in the data frame
			//If present writes to file
			if (data.getState().size() != 0) { 
				row.add(data.getState().get(i));
			}
			if (data.getGender().size() != 0) {
				row.add(data.getGender().get(i));
			}
			if (data.getYear().size() != 0) {
				row.add(String.valueOf(data.getYear().get(i)));
			}
			if (data.getName().size() != 0) {
				row.add(data.getName().get(i));
			}
			if (data.getCount().size() != 0) {
				row.add(String.valueOf(data.getCount().get(i)));
			}
		    
		    String collect = row.stream().collect(Collectors.joining(","));
		    
		    writer.write(collect + System.lineSeparator());
		
		}
		System.out.println("Successfully written to file!\n");
		writer.close();
	}
	
	/**
	 * Concatenates two MyDataFrames and returns one MyDataFrame
	 * @param df1 The first MyDataFrame to be concatenated
	 * @param df2 The second MyDataFrame to be concatenated
	 * @return concatenated MyDataFrame
	 */
	public MyDataFrame concat(MyDataFrame df1, MyDataFrame df2) {
		
		//Define new ArrayLists to store combined column values of the two MyDataFrames
		//Appends corresponding columns from the two MyDataFrames
		ArrayList<String> new_state = new ArrayList<String>();
		new_state.addAll(df1.getState());
		new_state.addAll(df2.getState());
		
		ArrayList<String> new_gender = new ArrayList<String>();
		new_gender.addAll(df1.getGender());
		new_gender.addAll(df2.getGender());
		
		ArrayList<Integer> new_year = new ArrayList<Integer>();
		new_year.addAll(df1.getYear());
		new_year.addAll(df2.getYear());
		
		ArrayList<String> new_name = new ArrayList<String>();
		new_name.addAll(df1.getName());
		new_name.addAll(df2.getName());
		
		ArrayList<Integer> new_count = new ArrayList<Integer>();
		new_count.addAll(df1.getCount());
		new_count.addAll(df2.getCount());
		
		//Size of new MyDataFrame
		int n = df1.getSize() + df2.getSize();
		
		//Creates a new MyDataFrame from the combined columns created above
		MyDataFrame new_data = new MyDataFrame(n,df1.getHeader(),new_state,new_gender,new_year,new_name,new_count);
		
		return new_data;
		
	}

}
