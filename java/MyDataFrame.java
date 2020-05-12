package java_project;

/**Importing Libraries**/
import java.util.*;
import java.util.stream.Collectors;

/**********************************************************************************
 * Implementing the MyDataFrame class to mimic some functionalities provided by Pandas DataFrames
 * MyDataFrame class provides the following interfaces:
 * 1. Head and Tail - Returns the top or bottom n rows 
 * 2. dType - Returns the type of the column specified by index
 * 3. Slicing - Returns the columns specified
 * 4. Filtering - Returns data filtered by applying “col op o” on MyDataFrame object, e.g. “count > 10”, “state = ‘IL’”
 * 5. Indexing - Returns the rows starting from or between specified index/label
 * 6. Sorting - Returns the data sorted by the column specified by index/name
 * 7. Aggregation - Returns the minimum/maximum element of the column specified by index/label
**********************************************************************************/

public class MyDataFrame {

	public List<String> header = new ArrayList<String>(); //Storing Header
	public Integer size; //Storing size of MyDataFrame
	private ArrayList<Integer> index = new ArrayList<Integer>(); //Store row indices
	
	//ArrayLists for specific columns
    private ArrayList<String> state = new ArrayList<String>(); 
    private ArrayList<String> gender =new ArrayList<String>();  
    private ArrayList<Integer> year = new ArrayList<Integer>();
    private ArrayList<String> name = new ArrayList<String>();  
    private ArrayList<Integer> count = new ArrayList<Integer>(); 
	

	/**
	 * Constructor
	 * @param n number of rows
	 * @param header column names
	 * @param state ArrayList of state field
	 * @param gender ArrayList of gender field
	 * @param year ArrayList of year field
	 * @param name ArrayList of name field
	 * @param count ArrayList of count field
	 */
	public MyDataFrame(Integer n, List<String> header, ArrayList<String> state, ArrayList<String> gender, ArrayList<Integer> year, ArrayList<String> name, ArrayList<Integer> count) {

		this.state = state;
		this.gender = gender;
		this.year = year;
		this.name = name;
		this.count = count;
		
		//Creates indices from 0 to size passed
		for (int i=0; i<n; i++) {
			this.index.add(i);
		}

		//Stores column names in 'header' in lower case
		for (String each : header) {
			this.header.add(each.trim().toLowerCase());
		}
		this.size = n;
	}
	
    
	/**
	 * Defining Getters and Setters
	*/
	//Index
	public ArrayList<Integer> getIndex(){
		return this.index; 
	}
	
	//Header
	public List<String> getHeader(){
		return this.header; 
	}
	
	//Size
	public Integer getSize(){
		return this.size; 
	}
	
	//State
	public ArrayList<String> getState(){
		return this.state; 
	}
	
	public void setState(ArrayList<String>  state){
		this.state = state;
	}
	
	//Gender 
	public ArrayList<String> getGender(){
		return this.gender; 
	}
	
	public void setGender(ArrayList<String> gender){
		this.gender = gender;
	}
	
	//Year
	public ArrayList<Integer> getYear(){
		return this.year; 
	}
	
	public void setYear(ArrayList<Integer> year){
		this.year = year;
	}
	
	//Name
	public ArrayList<String> getName(){
		return this.name; 
	}
	
	public void setName(ArrayList<String> name){
		this.name = name;
	}
	
	//Count 
	public ArrayList<Integer> getCount(){
		return this.count; 
	}
	
	public void setCount(ArrayList<Integer> count){
		this.count = count;
	}
	
	/**
	 * Returns the first n rows of the data
	 * @param n number of rows to be returned
	 * @return MyDataFrame with top n rows
	 */
	public MyDataFrame head(int n) {
		
		//Subsets Columns to get top n values
		ArrayList<String> state = new ArrayList<String>(this.getState().subList(0,n));
		ArrayList<String> gender = new ArrayList<String>(this.getGender().subList(0,n));
		ArrayList<Integer> year = new ArrayList<Integer>(this.getYear().subList(0,n));
		ArrayList<String> name = new ArrayList<String>(this.getName().subList(0,n));
		ArrayList<Integer> count = new ArrayList<Integer>(this.getCount().subList(0,n));
		
		//Creates a new MyDataFrame with column subsets created above
		MyDataFrame topN = new MyDataFrame(n,this.getHeader(),state,gender,year,name,count);
		
		return topN;		
	}
	
	/**
	 * Returns the bottom n rows of the data
	 * @param n number of rows to be returned
	 * @return MyDataFrame with bottom n rows
	 */
	public MyDataFrame tail(int n) {
		
		//Subsets Columns to get bottom n values
		ArrayList<String> state = new ArrayList<String>(this.getState().subList(this.getState().size() - n, this.getState().size()));
		ArrayList<String> gender = new ArrayList<String>(this.getGender().subList(this.getGender().size() - n, this.getGender().size()));
		ArrayList<Integer> year = new ArrayList<Integer>(this.getYear().subList(this.getYear().size() - n, this.getYear().size()));
		ArrayList<String> name = new ArrayList<String>(this.getName().subList(this.getName().size() - n, this.getName().size()));
		ArrayList<Integer> count = new ArrayList<Integer>(this.getCount().subList(this.getCount().size() - n, this.getCount().size()));
		
		//Creates a new MyDataFrame with column subsets created above
		MyDataFrame bottomN = new MyDataFrame(n,this.getHeader(),state,gender,year,name,count);
		
		return bottomN;		
	}
	
	/**
	 * Returns the type of the column specified by index. If the type is not uniform, returns ‘String’
	 * @param index/name Index/Name of the column in question
	 * @return datatype of the column at index/name, default 'String'
	 */
	public String dType(int index) {
		String type = "String";
		if (index == 2 || index == 4) {
			type = "Integer";
		}
		return type;
	}
	
	public String dType(String name) {
		String type = "String";
		if (name.equals("year") || name.equals("count")) {
			type = "Integer";
		}
		return type;
	}
	
	/**
	 * Returns the column specified by indicxes/column names.
	 * @param index/name/indexArray/nameArray
	 * @return MyDataFrame with only the column(s) specified
	 */
	public MyDataFrame slice(int index) {
		
		//Defining new ArrayLists to hold empty columns at first
		ArrayList<String> state = new ArrayList<String>();
		ArrayList<String> gender = new ArrayList<String>();
		ArrayList<Integer> year = new ArrayList<Integer>();
		ArrayList<String> name = new ArrayList<String>();
		ArrayList<Integer> count = new ArrayList<Integer>();
		List<String> header = new ArrayList<String>();
		
		//Depending on the index supplied
		//1. Adding the column name to the 'header'
		//2. Adding the corresponding column by invoking suitable getters
		if (index == 0) {
			header = Arrays.asList("state");
			state = this.getState();
		}
		else if (this.getHeader().get(index).equals("gender")) {
			header = Arrays.asList("gender");
			gender = this.getGender();
		}
		else if (this.getHeader().get(index).equals("year")) {
			header = Arrays.asList("year");
			year = this.getYear();
		}
		else if (this.getHeader().get(index).equals("name")) {
			header = Arrays.asList("name");
			name = this.getName();
		}
		else if (this.getHeader().get(index).equals("count")) {
			header = Arrays.asList("count");
			count = this.getCount();
		}
		
		//Creating a MyDataFrame with the column identified above
		MyDataFrame slice = new MyDataFrame(this.getSize(),header,state,gender,year,name,count);
		
		return slice;	
	}
	
	/**slice() with Column names**/
	public MyDataFrame slice(String col) {
		
		//Depending on the column name passed - calls the slice() method defined above with suitable index
		MyDataFrame slice = null;
		if (col.equals("state")) {
			slice = this.slice(0);
		}
		else if (col.equals("gender")) {
			slice = this.slice(1);
		}
		else if (col.equals("year")) {
			slice = this.slice(2);
		}
		else if (col.equals("name")) {
			slice = this.slice(3);
		}
		else if (col.equals("count")) {
			slice = this.slice(4);
		}
		
		return slice;	
	}
	
	/**slice() with array of column indices**/
	public MyDataFrame slice(int[] indexArr) {
		
		//Defining new ArrayLists to hold empty columns at first
		ArrayList<String> state = new ArrayList<String>();
		ArrayList<String> gender = new ArrayList<String>();
		ArrayList<Integer> year = new ArrayList<Integer>();
		ArrayList<String> name = new ArrayList<String>();
		ArrayList<Integer> count = new ArrayList<Integer>();
		List<String> header = new ArrayList<String>();
		
		//For each of the indices passed,
		//1. Identifies the suitable column names to be put in header
		//2. Adds column values to empty columns defined above
		for (int i: indexArr) {
			header.add(this.getHeader().get(i));
			
			if (i==0) {
				state = this.getState();
			}
			else if (this.getHeader().get(i).equals("gender")) {
				gender = this.getGender();
			}
			else if (this.getHeader().get(i).equals("year")) {
				year = this.getYear();
			}
			else if (this.getHeader().get(i).equals("name")) {
				name = this.getName();
			}
			else if (this.getHeader().get(i).equals("count")) {
				count = this.getCount();
			}
		}
		
		//Creates a new MyDataFrame with a subset of columns
		MyDataFrame slice = new MyDataFrame(this.getSize(),header,state,gender,year,name,count);
		
		return slice;		
	}
	
	/**slice() with Array of Column Names**/
	public MyDataFrame slice(String[] nameArr) {
		
		//Defining new ArrayLists to hold empty columns at first
		ArrayList<String> state = new ArrayList<String>();
		ArrayList<String> gender = new ArrayList<String>();
		ArrayList<Integer> year = new ArrayList<Integer>();
		ArrayList<String> name = new ArrayList<String>();
		ArrayList<Integer> count = new ArrayList<Integer>();
		List<String> header = new ArrayList<String>();
		
		//For each of the column names passed,
		//1. Adds column names to header
		//2. Adds column values to empty columns defined above
		for (String col : nameArr) {
			header.add(col);
			
			if (col.equals("state")) {
				state = this.getState();
			}
			else if (col.equals("gender")) {
				gender = this.getGender();
			}
			else if (col.equals("year")) {
				year = this.getYear();
			}
			else if (col.equals("name")) {
				name = this.getName();
			}
			else if (col.equals("count")) {
				count = this.getCount();
			}
		}
		
		//Creates a new MyDataFrame with a subset of columns
		MyDataFrame slice = new MyDataFrame(this.getSize(),header,state,gender,year,name,count);
		
		return slice;		
	}
	
	/**
	 * Returns data filtered by applying “col op o” on MyDataFrame object, e.g. “count > 10”, “state = ‘IL’”
	 * @param col Column on which to be filtered
	 * @param op Logical operation: ==,!=,<,<=,>,>=
	 * @param o Value to be compared with
	 * @return Filtered MyDataFrame
	 */
	public MyDataFrame filter(String col, String op, Object o) {
		
		int n = 0; //store size of filtered MyDataFrame
		
		//Defining new ArrayLists to hold empty columns at first
		ArrayList<String> state = new ArrayList<String>();
		ArrayList<String> gender = new ArrayList<String>();
		ArrayList<Integer> year = new ArrayList<Integer>();
		ArrayList<String> name = new ArrayList<String>();
		ArrayList<Integer> count = new ArrayList<Integer>();
		
		//Depending on datatype of the column, filtering will have to use different methods
		//If column passed is of type 'String', values are set to ArrayList 'word'
		//If column passed is of type 'Integer', values are set to ArrayList 'num'
		ArrayList<String> word = new ArrayList<String>();
		ArrayList<Integer> num = new ArrayList<Integer>();
		
		if (col.toLowerCase().equals("state")) {
			word = this.getState();
		}
		
		else if (col.toLowerCase().equals("name")) {
			word = this.getName();
		}
		
		else if (col.toLowerCase().equals("gender")) {
			word = this.getGender();
		}
		
		else if (col.toLowerCase().equals("year")) {
			num = this.getYear();
		}
		
		else if (col.toLowerCase().equals("count")) {
			num = this.getCount();
		}
		
		//If column passed is String => word is populated
		if (!word.isEmpty()) {
			String obj = (String) o; //Converting threshold to String for comparison
			for(int i=0; i < this.getSize(); i++) { //Scans each row of the DataFrame
				//Define switch case on the basis of the operation requested
				//Compare value of 'word'(which has the values of the column in question) with threshold value
				//If filter is True => appends corresponding i-th values of all columns
				//Increments n every time a row validates filter condition
				switch(op) {
					case "==" :
						if(word.get(i).compareTo(obj) == 0) {
							state.add(this.getState().get(i));
							gender.add(this.getGender().get(i));
							year.add(this.getYear().get(i));
							name.add(this.getName().get(i));
							count.add(this.getCount().get(i));
							n++;
						}
					break;
					
					case "!=" :
						if(word.get(i).compareTo(obj) != 0) {
							state.add(this.getState().get(i));
							gender.add(this.getGender().get(i));
							year.add(this.getYear().get(i));
							name.add(this.getName().get(i));
							count.add(this.getCount().get(i));
							n++;
						}
					break;
					
					case "<" :
						if(word.get(i).compareTo(obj) < 0) {
							state.add(this.getState().get(i));
							gender.add(this.getGender().get(i));
							year.add(this.getYear().get(i));
							name.add(this.getName().get(i));
							count.add(this.getCount().get(i));
							n++;
						}
					break;
					
					case "<=" :
						if(word.get(i).compareTo(obj) <= 0) {
							state.add(this.getState().get(i));
							gender.add(this.getGender().get(i));
							year.add(this.getYear().get(i));
							name.add(this.getName().get(i));
							count.add(this.getCount().get(i));
							n++;
						}
					break;
					
					case ">" :
						if(word.get(i).compareTo(obj) > 0) {
							state.add(this.getState().get(i));
							gender.add(this.getGender().get(i));
							year.add(this.getYear().get(i));
							name.add(this.getName().get(i));
							count.add(this.getCount().get(i));
							n++;
						}
					break;
					
					case ">=" :
						if(word.get(i).compareTo(obj) >= 0) {
							state.add(this.getState().get(i));
							gender.add(this.getGender().get(i));
							year.add(this.getYear().get(i));
							name.add(this.getName().get(i));
							count.add(this.getCount().get(i));
							n++;
						}
					break;
				}			
			}
		}
		
		//If column passed is Integer => num is populated
		//Remaining steps same as above
		if (!num.isEmpty()) {
			Integer obj = Integer.valueOf((String) o);
			for(int i=0; i < this.getSize(); i++) {
				switch(op) {
					case "==" :
						if(num.get(i)==obj) {
							state.add(this.getState().get(i));
							gender.add(this.getGender().get(i));
							year.add(this.getYear().get(i));
							name.add(this.getName().get(i));
							count.add(this.getCount().get(i));
							n++;
						}
					break;
					
					case "!=" :
						if(num.get(i)!=obj) {
							state.add(this.getState().get(i));
							gender.add(this.getGender().get(i));
							year.add(this.getYear().get(i));
							name.add(this.getName().get(i));
							count.add(this.getCount().get(i));
							n++;
						}
					break;
					
					case "<" :
						if(num.get(i)<obj) {
							state.add(this.getState().get(i));
							gender.add(this.getGender().get(i));
							year.add(this.getYear().get(i));
							name.add(this.getName().get(i));
							count.add(this.getCount().get(i));
							n++;
						}
					break;
					
					case "<=" :
						if(num.get(i)<=obj) {
							state.add(this.getState().get(i));
							gender.add(this.getGender().get(i));
							year.add(this.getYear().get(i));
							name.add(this.getName().get(i));
							count.add(this.getCount().get(i));
							n++;
						}
					break;
					
					case ">" :
						if(num.get(i)>obj) {
							state.add(this.getState().get(i));
							gender.add(this.getGender().get(i));
							year.add(this.getYear().get(i));
							name.add(this.getName().get(i));
							count.add(this.getCount().get(i));
							n++;
						}
					break;
					
					case ">=" :
						if(num.get(i)>=obj) {
							state.add(this.getState().get(i));
							gender.add(this.getGender().get(i));
							year.add(this.getYear().get(i));
							name.add(this.getName().get(i));
							count.add(this.getCount().get(i));
							n++;
						}
					break;
				}			
			}
		}
		
		//Create new MyDataFrame with filtered column values
		MyDataFrame myloc = new MyDataFrame(n,this.getHeader(),state,gender,year,name,count);
		
		return myloc;
	}
	
	/**
	 * Returns the rows starting from row index or row label; or between two indices or labels
	 * @param index Row index
	 * @return MyDataFrame with rows starting from 'index'
	 */
	public MyDataFrame loc(int index) {
		//Subsetting column array lists to get rows after 'index' supplied
		ArrayList<String> state = new ArrayList<String>(this.getState().subList(index, this.getSize()));
		ArrayList<String> gender = new ArrayList<String>(this.getGender().subList(index, this.getSize()));
		ArrayList<Integer> year = new ArrayList<Integer>(this.getYear().subList(index, this.getSize()));
		ArrayList<String> name = new ArrayList<String>(this.getName().subList(index, this.getSize()));
		ArrayList<Integer> count = new ArrayList<Integer>(this.getCount().subList(index, this.getSize()));
		
		//Number of rows
		int n = this.getSize() - index ; 
		
		//Create data frame 
		MyDataFrame myloc = new MyDataFrame(n,this.getHeader(),state,gender,year,name,count);
		
		return myloc;
	}
	
	/**
	 * Returns the rows starting from label.
	 * @param label row label
	 * @return MyDataFrame with rows starting from 'label'
	 */
	public MyDataFrame loc(String label) {
		
		//Initialize array lists 
		ArrayList<String> state = new ArrayList<String>();
		ArrayList<String> gender = new ArrayList<String>();
		ArrayList<Integer> year = new ArrayList<Integer>();
		ArrayList<String> name = new ArrayList<String>();
		ArrayList<Integer> count = new ArrayList<Integer>();
		
		//If MyDataFrame to be subset has integer indices
		//Converting label passed to integer and calling the loc() function defined above
		if(this.getIndex().get(0) instanceof Integer) {
			int n = Integer.valueOf(label);
			MyDataFrame myloc = this.loc(n);
			return myloc;
		}
		
		//If MyDataFrame to be subset has string labels
		//Comparing label of each row with the label supplied
		//If a row has label >= the label provided, adding that row to the resulting dataframe columns
		else {
			//Loop through and find all rows with matching values 
			for(int i=0; i < this.getIndex().size(); i++) {
				if(this.getIndex().get(i).toString().compareTo(label)>=0) {
					state.add(this.getState().get(i));
					gender.add(this.getGender().get(i));
					year.add(this.getYear().get(i));
					name.add(this.getName().get(i));
					count.add(this.getCount().get(i));
				}
			}
			
			//Number of rows
			int n = state.size(); 

			//Create data frame 
			MyDataFrame myloc = new MyDataFrame(n,this.getHeader(),state,gender,year,name,count);
					
			return myloc;
		}
		
	}
	
	/**
	 * Returns the rows between from and to(including from and to)
	 * @param from Starting row index
	 * @param to Finishing row index
	 * @return MyDataFrame with rows between 'from' and 'to'
	 */
	public MyDataFrame loc(int from, int to){
		to = to + 1; //So that 'to' row is included
		//Subsetting column array lists to get rows between 'from' and 'to' indices supplied
		ArrayList<String> state = new ArrayList<String>(this.getState().subList(from, to));
		ArrayList<String> gender = new ArrayList<String>(this.getGender().subList(from, to));
		ArrayList<Integer> year = new ArrayList<Integer>(this.getYear().subList(from, to));
		ArrayList<String> name = new ArrayList<String>(this.getName().subList(from, to));
		ArrayList<Integer> count = new ArrayList<Integer>(this.getCount().subList(from, to));
				
		//Number of rows 
		int n = to - from; 
				
		//Create data frame 
		MyDataFrame myloc = new MyDataFrame(n,this.getHeader(),state,gender,year,name,count);
		
		return myloc;		
	}

	/**
	 * Returns the rows between from and to(including from and to) labels
	 * @param from row label from
	 * @param to row label to
	 * @return MyDataFrame with rows between 'from' and 'to'
	 */
	public MyDataFrame loc(String from, String to) {
		
		//If MyDataFrame to be subset has integer indices
		//Converting labels passed to integers and calling the loc(int from, int to) function defined above
		if(this.getIndex().get(0) instanceof Integer) {
			int i_from = Integer.valueOf(from);
			int i_to = Integer.valueOf(to); 
			MyDataFrame myloc = this.loc(i_from,i_to);			
			return myloc;
		}
		
		//If MyDataFrame to be subset has string labels
		//Comparing label of each row with the label supplied
		//If a row has 'from' <= label <= 'to', adding that row to the resulting dataframe columns
		else {
			//Initialize array lists 
			ArrayList<String> state = new ArrayList<String>();
			ArrayList<String> gender = new ArrayList<String>();
			ArrayList<Integer> year = new ArrayList<Integer>();
			ArrayList<String> name = new ArrayList<String>();
			ArrayList<Integer> count = new ArrayList<Integer>();
			
			//Looping through all the rows
			for(int i=0; i < this.getIndex().size(); i++) {
				if(this.getIndex().get(i).toString().compareTo(from)>=0 & this.getIndex().get(i).toString().compareTo(to)<=0) {
					state.add(this.getState().get(i));
					gender.add(this.getGender().get(i));
					year.add(this.getYear().get(i));
					name.add(this.getName().get(i));
					count.add(this.getCount().get(i));
				}
			}
			
			//Number of rows
			int n = state.size(); 

			//Create data frame 
			MyDataFrame myloc = new MyDataFrame(n,this.getHeader(),state,gender,year,name,count);
					
			return myloc;
		}
		
	}

	/**
	 * Defining private methods of Bubble Sort to sort a column and return the original indices of the sorted column
	 * These indices are then utilized to sort the remaining fields according to the Column to be sorted
	 * Separate bubbleSort methods are defined for String columns and Integer columns
	 * @param array Column to be sorted
	 * @return Original indices of the sorted column
	 */
	private int[] bubbleSort_int(ArrayList<Integer> array) {
		
		int[] index = new int[array.size()];

		ArrayList<Integer> name = new ArrayList<Integer>(array);
		for(int i=0; i < name.size(); i++) {
			index[i] = i; //Indices of Array passed
		}
		for(int i=0; i < name.size(); i++) {
			for(int j = 0; j < name.size() - i - 1; j++) {
				if (name.get(j) > name.get(j+1)) { //Compare every successive elements and swaps if found in reverse order
					int temp = name.get(j);
					name.set(j, name.get(j+1));
					name.set(j+1, temp);
					//Every time the elements of the array are flipped, elements of the index array are also flipped
					int temp_ind = index[j];
					index[j] = index[j+1];
					index[j+1] = temp_ind;					
				}
			}			
		}
		return index;
	}
	
	/**bubbleSort for Integer columns**/
	private int[] bubbleSort_str(ArrayList<String> array) {
		
		int[] index = new int[array.size()];

		ArrayList<String> name = new ArrayList<String>(array);
		for(int i=0; i < name.size(); i++) {
			index[i] = i;
		}
		for(int i=0; i < name.size(); i++) {
			for(int j = 0; j < name.size() - i - 1; j++) {
				if (name.get(j).compareTo(name.get(j+1)) > 0) {
					String temp = name.get(j);
					name.set(j, name.get(j+1));
					name.set(j+1, temp);
					int temp_ind = index[j];
					index[j] = index[j+1];
					index[j+1] = temp_ind;					
				}
			}			
		}
		return index;
	}
	
	/**
	 * Returns the data sorted by the column specified
	 * @param name/index to identify the column to sort by
	 * @return MyDataFrame sorted by column specified
	 */
	public MyDataFrame sort(String name) {
		
		//Array of integers to store the original indices of the Sorted column
		int[] sort_index = null;
		
		//Uses bubbleSort() defined above to sort the column passed and return the original indices of the Sorted Array
		switch(name.toLowerCase()) {
			case "state":
				sort_index = this.bubbleSort_str(this.getState());
			break;
			case "name":
				sort_index = this.bubbleSort_str(this.getName());
			break;	
			case "gender":
				sort_index = this.bubbleSort_str(this.getGender());
			break;	
			case "year":
				sort_index = this.bubbleSort_int(this.getYear());
			break;	
			case "count":
				sort_index = this.bubbleSort_int(this.getCount());
			break;	
		}
		
		//Initialize array lists 
		ArrayList<String> state = new ArrayList<String>();
		ArrayList<String> gender = new ArrayList<String>();
		ArrayList<Integer> year = new ArrayList<Integer>();
		ArrayList<String> name1 = new ArrayList<String>();
		ArrayList<Integer> count = new ArrayList<Integer>();
		
		//Appending values in each of the columns in order of the original indices of the sorted column
		for(int i : sort_index) {
			state.add(this.getState().get(i));
			gender.add(this.getGender().get(i));
			year.add(this.getYear().get(i));
			name1.add(this.getName().get(i));
			count.add(this.getCount().get(i));
		}
		//Create data frame 
		MyDataFrame mySort = new MyDataFrame(this.getSize(),this.getHeader(),state,gender,year,name1,count);
				
		return mySort;
	}
	
	/**Sort() with column index
	 * Depending on column index, calls sort(String name) defined above with suitable solumn name
	 * @param index Column index
	 * @return Returns the data sorted by the column specified by index
	 */
	public MyDataFrame sort(int index) {
		MyDataFrame mySort = null;
		switch(index) {
			case 0:
				mySort = this.sort("state");
			break;
			case 1:
				mySort = this.sort("gender");
			break;	
			case 2:
				mySort = this.sort("year");
			break;	
			case 3:
				mySort = this.sort("name");
			break;	
			case 4:
				mySort = this.sort("count");
			break;	
		}
		return mySort;
	}
	
	/**
	 * Returns minimum/maximum element in the column specified by index/name
	 * @param index/name Column index/name to be aggregated
	 * @return min/max value of the column
	 */
	public Object getMin(int index) {
		Object min = null;
		switch(index) {
			case 0:
				min = Collections.min(this.getState());
			break;
			case 1:
				min = Collections.min(this.getGender());
			break;
			case 2:
				min = Collections.min(this.getYear());
			break;
			case 3:
				min = Collections.min(this.getName());
			break;
			case 4:
				min = Collections.min(this.getCount());
			break;
		}
		return min;
	}
	
	public Object getMin(String label) {
		Object min = null;
		switch(label.toLowerCase()) {
			case "state":
				min = Collections.min(this.getState());
			break;
			case "gender":
				min = Collections.min(this.getGender());
			break;
			case "year":
				min = Collections.min(this.getYear());
			break;
			case "name":
				min = Collections.min(this.getName());
			break;
			case "count":
				min = Collections.min(this.getCount());
			break;
		}
		return min;
	}
	
	public Object getMax(int index) {
		Object max = null;
		switch(index) {
			case 0:
				max = Collections.max(this.getState());
			break;
			case 1:
				max = Collections.max(this.getGender());
			break;
			case 2:
				max = Collections.max(this.getYear());
			break;
			case 3:
				max = Collections.max(this.getName());
			break;
			case 4:
				max = Collections.max(this.getCount());
			break;
		}
		return max;
	}
	
	public Object getMax(String label) {
		Object max = null;
		switch(label.toLowerCase()) {
			case "state":
				max = Collections.max(this.getState());
			break;
			case "gender":
				max = Collections.max(this.getGender());
			break;
			case "year":
				max = Collections.max(this.getYear());
			break;
			case "name":
				max = Collections.max(this.getName());
			break;
			case "count":
				max = Collections.max(this.getCount());
			break;
		}
		return max;
	}
	
	/**
	 * Returns aggregated minimum/maximum element in the column specified by index/name
	 * @param index/name Column index/name to be aggregated
	 * @return min/max value in a column 
	 */
		public Object getMin_agg(int index) {
			ArrayList<String> word = new ArrayList<String>();
			ArrayList<Integer> number = new ArrayList<Integer>();
			Object obj_out = null;
			//Which column 
			if (index == 0) {
				word = this.getState();
			}
			else if (index == 1) {
				word = this.getGender();
			}
			else if (index == 2) {
				number = this.getYear();
			}
			else if (index == 3) {
				word = this.getName();
			}
			else if (index == 4) {
				number = this.getCount();	
			}
			//Columns of type string
			if(word.isEmpty() == false) {
				ArrayList<String> unique_words = (ArrayList<String>) word.stream().distinct().collect(Collectors.toList()); 
				int freq_init = Collections.frequency(word, unique_words.get(0)); 
				String min = unique_words.get(0); 
				String of_interest; 
				for(int i = 1; i < unique_words.size(); i++ ) {
					of_interest = unique_words.get(i); 
					int freq = Collections.frequency(word, of_interest);
					if(freq < freq_init) {
						min = unique_words.get(i); 
					}
				}
				obj_out= min ; 
			}
			//Columns of type integer 
			if(number.isEmpty() == false) {
				ArrayList<Integer> unique_numbers = (ArrayList<Integer>) number.stream().distinct().collect(Collectors.toList());	
				int freq_init = Collections.frequency(number, unique_numbers.get(0)); 
				int min = unique_numbers.get(0); 
				int of_interest; 
				for(int i = 1; i < unique_numbers.size(); i++ ) {
					of_interest = unique_numbers.get(i); 
					int freq = Collections.frequency(word, of_interest);
					if(freq < freq_init) {
						min = unique_numbers.get(i); 
					}
				}
		
				obj_out = min; 			
			}
			
			return obj_out; 		
		}
	
	
	public Object getMax_agg(int index) {
		ArrayList<String> word = new ArrayList<String>();
		ArrayList<Integer> number = new ArrayList<Integer>();
		Object obj_out = null;
		//Which column
		if (index == 0) {
			word = this.getState();
		}
		else if (index == 1) {
			word = this.getGender();
		}
		else if (index == 2) {
			number = this.getYear();
		}
		else if (index == 3) {
			word = this.getName();
		}
		else if (index == 4) {
			number = this.getCount();	
		}
		//Columns of type string
		if(word.isEmpty() == false) {
			ArrayList<String> unique_words = (ArrayList<String>) word.stream().distinct().collect(Collectors.toList()); 
			int freq_init = Collections.frequency(word, unique_words.get(0)); 
			String max = unique_words.get(0); 
			String of_interest; 
			for(int i = 1; i < unique_words.size(); i++ ) {
				of_interest = unique_words.get(i); 
				int freq = Collections.frequency(word, of_interest);
				if(freq > freq_init) {
					max = unique_words.get(i); 
				}
			}
			obj_out= max ; 
		}
		//Columns of type integer
		if(number.isEmpty() == false) {
			ArrayList<Integer> unique_numbers = (ArrayList<Integer>) number.stream().distinct().collect(Collectors.toList());	
			int freq_init = Collections.frequency(number, unique_numbers.get(0)); 
			int max = unique_numbers.get(0); 
			int of_interest; 
			for(int i = 1; i < unique_numbers.size(); i++ ) {
				of_interest = unique_numbers.get(i); 
				int freq = Collections.frequency(word, of_interest);
				if(freq > freq_init) {
					max = unique_numbers.get(i); 
				}
			}
	
			obj_out = max; 			
		}
		
		return obj_out; 		
	}
	
	public Object getMin_agg(String label) {
		ArrayList<String> word = new ArrayList<String>();
		ArrayList<Integer> number = new ArrayList<Integer>();
		Object obj_out = null;
		//Which column 
		if (label.equals("state") | label.equals("State") ) {
			word = this.getState();
		}
		else if (label.equals("gender") | label.equals("Gender")) {
			word = this.getGender(); 
		}
		else if (label.equals("year") | label.equals("Year")) {
			number = this.getYear();
		}
		else if (label.equals("name") | label.equals("Name")) {
			word = this.getName();
		}
		else if (label.equals("count") | label.equals("Count")) {
			number = this.getCount();
		}
		//Columns of type stirng 
		if(word.isEmpty() == false) {
			ArrayList<String> unique_words = (ArrayList<String>) word.stream().distinct().collect(Collectors.toList()); 
			int freq_init = Collections.frequency(word, unique_words.get(0)); 
			String min = unique_words.get(0); 
			String of_interest; 
			for(int i = 1; i < unique_words.size(); i++ ) {
				of_interest = unique_words.get(i); 
				int freq = Collections.frequency(word, of_interest);
				if(freq < freq_init) {
					min = unique_words.get(i); 
				}
			}
			obj_out= min ; 
		}
		//Columns of type integer 
		if(number.isEmpty() == false) {
			ArrayList<Integer> unique_numbers = (ArrayList<Integer>) number.stream().distinct().collect(Collectors.toList());	
			int freq_init = Collections.frequency(number, unique_numbers.get(0)); 
			int min = unique_numbers.get(0); 
			int of_interest; 
			for(int i = 1; i < unique_numbers.size(); i++ ) {
				of_interest = unique_numbers.get(i); 
				int freq = Collections.frequency(word, of_interest);
				if(freq < freq_init) {
					min = unique_numbers.get(i); 
				}
			}
			obj_out = min; 
		}
		return obj_out; 		
	}

	
	public Object getMax_agg(String label) {
		ArrayList<String> word = new ArrayList<String>();
		ArrayList<Integer> number = new ArrayList<Integer>();
		Object obj_out = null;
		//Which column 
		if (label.equals("state") | label.equals("State") ) {
			word = this.getState();
		}
		else if (label.equals("gender") | label.equals("Gender")) {
			word = this.getGender(); 
		}
		else if (label.equals("year") | label.equals("Year")) {
			number = this.getYear();
		}
		else if (label.equals("name") | label.equals("Name")) {
			word = this.getName();
		}
		else if (label.equals("count") | label.equals("Count")) {
			number = this.getCount();
		}
		//Columns of type string 
		if(word.isEmpty() == false) {
			ArrayList<String> unique_words = (ArrayList<String>) word.stream().distinct().collect(Collectors.toList()); 
			int freq_init = Collections.frequency(word, unique_words.get(0)); 
			String max = unique_words.get(0); 
			String of_interest; 
			for(int i = 1; i < unique_words.size(); i++ ) {
				of_interest = unique_words.get(i); 
				int freq = Collections.frequency(word, of_interest);
				if(freq > freq_init) {
					max = unique_words.get(i); 
				}
			}
			obj_out= max ; 
		}
		//Columns of type integer 
		if(number.isEmpty() == false) {
			ArrayList<Integer> unique_numbers = (ArrayList<Integer>) number.stream().distinct().collect(Collectors.toList());	
			int freq_init = Collections.frequency(number, unique_numbers.get(0)); 
			int max = unique_numbers.get(0); 
			int of_interest; 
			for(int i = 1; i < unique_numbers.size(); i++ ) {
				of_interest = unique_numbers.get(i); 
				int freq = Collections.frequency(word, of_interest);
				if(freq > freq_init) {
					max = unique_numbers.get(i); 
				}
			}
	
			obj_out = max; 			
		}
		return obj_out; 		
	}

	/**
	 * Formatting and Printing the MyDataFrame
	 */
	public void print() {
		
		List<String> heading = this.getHeader();
		String head = heading.stream().collect(Collectors.joining("\t"));	    
		System.out.println(head);
		
		for(int i = 0; i < this.getSize(); i++) {
		    
			List<String> row = new ArrayList<>();
			
			if (this.getState().size() != 0) {
				row.add(this.getState().get(i));
			}
			if (this.getGender().size() != 0) {
				row.add(this.getGender().get(i));
			}
			if (this.getYear().size() != 0) {
				row.add(String.valueOf(this.getYear().get(i)));
			}
			if (this.getName().size() != 0) {
				row.add(this.getName().get(i));
			}
			if (this.getCount().size() != 0) {
				row.add(String.valueOf(this.getCount().get(i)));
			}
			
		    String collect = row.stream().collect(Collectors.joining("\t"));
		    
		    System.out.println(collect);
		
		}
	}
}
