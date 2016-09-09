import java.io.RandomAccessFile;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.*;
import java.util.Scanner;
import java.util.TreeMap;

public class fileindexing1 {
	
	static String prompt = "chandusql> ";

	static String nameOfSchema="information_schema";
	static TreeMap<Object, ArrayList> map1 = new TreeMap();
	static int numberOfRows=0;

	public static void main(String[] Args) {
		/* Display the welcome splash screen */
		fileindexing1 methodsOfDB=new fileindexing1();
		MakeInformationSchema.main(Args);
		methodsOfDB.splashScreen();
		
				Scanner scanner = new Scanner(System.in).useDelimiter(";");
		String userCommand; 

		do {  // do-while !exit
			System.out.print(prompt);
			userCommand = scanner.next().trim();
			
			
			String[] userCommanddelimited = userCommand.split(" ");

			if(userCommanddelimited[0].equalsIgnoreCase("SHOW") && userCommanddelimited[1].equalsIgnoreCase("SCHEMAS"))
			{
				methodsOfDB.showAllSchemas();
			}
			else if(userCommanddelimited[0].equalsIgnoreCase("SHOW") && userCommanddelimited[1].equalsIgnoreCase("TABLES"))
			{
				methodsOfDB.showSchemaTables();
			}
			else if(userCommanddelimited[0].equalsIgnoreCase("USE"))
			{
				methodsOfDB.useSchema(userCommand);
			}
			
			else if(userCommanddelimited[0].equalsIgnoreCase("CREATE") && userCommanddelimited[1].equalsIgnoreCase("TABLE"))
			{
				methodsOfDB.tableCreate(userCommand);
			}
			else if(userCommanddelimited[0].equalsIgnoreCase("INSERT"))
			{
				methodsOfDB.insert(userCommand);
			}
			
			else if(userCommanddelimited[0].equalsIgnoreCase("CREATE") && userCommanddelimited[1].equalsIgnoreCase("SCHEMA"))
			{
				methodsOfDB.createSchema(userCommand);
			}
			else if(userCommanddelimited[0].equalsIgnoreCase("SELECT")){
				if( userCommand.contains("where") || userCommand.contains("WHERE") )
					methodsOfDB.selectWhere(userCommand);
				else
					methodsOfDB.select(userCommand);
			}
			
			
			else if(userCommand.equalsIgnoreCase("exit"))
			{

			}
		}while(!userCommand.equals("exit"));
		System.out.println("Exiting...");
	} /* End main() method */


	//  ===========================================================================
	//  STATIC METHOD DEFINTIONS BEGIN HERE
	//  ===========================================================================

	private void showAllSchemas() {
		

		try {
			RandomAccessFile schemata = new RandomAccessFile("information_schema.schemata.tbl", "rw");
			for(int i=0;i<schemata.length();i++){
				if(schemata.getFilePointer()<schemata.length()){
					byte length= schemata.readByte();
					for(int j=0;j<length;j++)
						System.out.print((char)schemata.readByte());
					System.out.println();

				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	private void createSchema(String command) {
		// TODO Auto-generated method stub
		RandomAccessFile schemata;
		try {
			String schemaname=command;
			if(schemaname != null){
				String[] usercommandsplitarray = schemaname.split(" ");
				nameOfSchema=usercommandsplitarray[2];
				
				incrementRows("information_schema","schemata");
				schemata= new RandomAccessFile("information_schema.schemata.tbl", "rw");
				schemata.seek(schemata.length());
				schemata.writeByte(nameOfSchema.length());
				schemata.writeBytes(nameOfSchema);
				System.out.println("Schema "+ nameOfSchema +" Created");
				nameOfSchema="information_schema";
			}

		} catch (Exception e) {
			e.printStackTrace();
		}


	}

	private void selectWhere(String userCommand) {
		String type;
		String typeOfColumn;
		String[] strarraydelimited = userCommand.split("[ =\\>\\<]");
		ArrayList<ArrayList<String>> arr = new ArrayList<ArrayList<String>>(); 
		ArrayList<String> columnarraylist = new ArrayList<String>();
		ArrayList<String> arraylisttype = new ArrayList<String>();
		String[] usercommandsplittable = userCommand.split(" ");
		String nameOfTable =usercommandsplittable[3];
		String columnPrimaryKey;
		String valuecheck;
		String whereoperator ="";
		int samplenumber = -1;
		
		
		
		try{
			columnPrimaryKey=strarraydelimited[5];
			valuecheck=strarraydelimited[strarraydelimited.length-1];
			if(userCommand.contains("=")){
				whereoperator="=";
			}
			if(userCommand.contains("<")){
				whereoperator = "<";
			}
			if(userCommand.contains(">")){
				whereoperator = ">";
			}
			
			arr = getColumnName(nameOfTable);
			columnarraylist=arr.get(0);
			arraylisttype=arr.get(1);
			displayLine();
			for(int i=0;i<columnarraylist.size();i++){
				System.out.print(columnarraylist.get(i)+" | ");
				if(columnarraylist.get(i).equalsIgnoreCase(columnPrimaryKey)){
					samplenumber=i;
				}
			}
			System.out.println();
			displayLine();
			if(samplenumber != -1){
				typeOfColumn=arraylisttype.get(samplenumber);
				
				type="";
				RandomAccessFile  tablename = new RandomAccessFile(nameOfSchema.toLowerCase()+"."+nameOfTable.toLowerCase()+".tbl","rw");

				ArrayList<ArrayList<String>> listToOut = new ArrayList<ArrayList<String>>();
				

				for(int i=0;i<tablename.length();i++)
				{
					if(tablename.getFilePointer()<tablename.length())
					{
						ArrayList<String> listToIn = new ArrayList<String>();
						for(int j=0;j<arraylisttype.size();j++)
						{
							type = (String) arraylisttype.get(j);

							if(type.equalsIgnoreCase("int"))
							{
								int k =tablename.readInt();
								
								listToIn.add(Integer.toString(k));
							}
							else if(type.equalsIgnoreCase("long") || type.equalsIgnoreCase("long int"))
							{
								long k= tablename.readLong();
								
								listToIn.add(Long.toString(k));
							}
							else if(type.equalsIgnoreCase("short") || type.equalsIgnoreCase("short int"))
							{
								short k = tablename.readShort();
								
								listToIn.add(Short.toString(k));
							}
							else if(type.equalsIgnoreCase("float"))
							{
								float k= tablename.readFloat();
				
								listToIn.add(Float.toString(k));
							}
							else if(type.equalsIgnoreCase("double"))
							{
								double k=tablename.readDouble();
								listToIn.add(Double.toString(k));
							}
							else if(type.equalsIgnoreCase("byte"))
							{
								byte k =tablename.readByte();
								listToIn.add(Byte.toString(k));
							}
							else 
							{	
								int length = tablename.readByte();
								String output="";
								for(int g=0; g< length ;g++)
								{
									output+= (char)tablename.readByte();
								}
								listToIn.add(output);
							}
						}

					
						listToOut.add(listToIn);
					}
				}
				int flag=0;
				ArrayList<Integer> integerlistarray = new ArrayList<Integer>();
				for(int k=0;k<listToOut.size();k++)
				{
					flag=0;
					ArrayList l = listToOut.get(k);
					// **** equals comparison ** //
					
					if(whereoperator.equals(">"))
					{
						int g = -1;
						int v = -1;
						if(typeOfColumn.equalsIgnoreCase("int")){
							v= Integer.parseInt(valuecheck);
							g=Integer.parseInt((String) l.get(samplenumber));
						}
						
						else if(typeOfColumn.equalsIgnoreCase("float")){
							float f= Float.parseFloat(valuecheck);
							float m = Float.parseFloat((String) l.get(samplenumber));
							if(m>f){
								integerlistarray.add(k);
							}
							flag=1;
						}
						else if(typeOfColumn.equalsIgnoreCase("double")){
							double f= Double.parseDouble(valuecheck);
							double m = Double.parseDouble((String) l.get(samplenumber));
							if(m>f){
								integerlistarray.add(k);
							}
							flag=1;
						}
						else if(typeOfColumn.equalsIgnoreCase("short")|| typeOfColumn.equalsIgnoreCase("short int")){
							v= Short.parseShort(valuecheck);
							g=Short.parseShort((String) l.get(samplenumber));
						}
						else if(typeOfColumn.equalsIgnoreCase("long")|| typeOfColumn.equalsIgnoreCase("long int")){
							long f= Long.parseLong(valuecheck);
							long m = Long.parseLong((String) l.get(samplenumber));
							if(m>f){
								integerlistarray.add(k);
							}
							flag=1;
						}
						else if(typeOfColumn.equalsIgnoreCase("byte")){
							byte f= Byte.parseByte(valuecheck);
							byte m = Byte.parseByte((String) l.get(samplenumber));
							if(m>f){
								integerlistarray.add(k);
							}
							flag=1;
						}
						else{
							String checkStr="";
							if(valuecheck.contains("\'") || valuecheck.contains("\"")){
								checkStr=valuecheck.substring(1, valuecheck.length()-1);
							}
							else{
								checkStr=valuecheck;
							}
							int p=l.get(samplenumber).toString().toLowerCase().compareTo(checkStr.toLowerCase());
							if(p>0)
							{
								integerlistarray.add(k);
							}
							flag=1;
						}

						if(g>v && flag==0)
						{
							integerlistarray.add(k);
						}

					}
					if(whereoperator.equals("="))
					{
						String samplestr="";
						if(valuecheck.contains("\'") || valuecheck.contains("\"")){
							samplestr=valuecheck.substring(1, valuecheck.length()-1);
						}
						else{
							samplestr=valuecheck;
						}
						
						int p=l.get(samplenumber).toString().toLowerCase().compareTo(samplestr.toLowerCase());
						if(p==0)
						{
							integerlistarray.add(k);
						}
					}
					if(whereoperator.equals("<"))
					{
						int intvar1 = -1;
						int intvar2 = -1;
						if(typeOfColumn.equalsIgnoreCase("int")){
							intvar2= Integer.parseInt(valuecheck);
							intvar1=Integer.parseInt((String) l.get(samplenumber));
						}
						
						else if(typeOfColumn.equalsIgnoreCase("long")|| typeOfColumn.equalsIgnoreCase("long int")){
							long f= Long.parseLong(valuecheck);
							long m = Long.parseLong((String) l.get(samplenumber));
							if(m<f){
								integerlistarray.add(k);
							}
		
							flag=1;
						}
						else if(typeOfColumn.equalsIgnoreCase("float")){
							float f= Float.parseFloat(valuecheck);
							float m = Float.parseFloat((String) l.get(samplenumber));
							if(m<f){
								integerlistarray.add(k);
							}
							flag=1;
						}
						else if(typeOfColumn.equalsIgnoreCase("short")|| typeOfColumn.equalsIgnoreCase("short int")){
							intvar2= Short.parseShort(valuecheck);
							intvar1=Short.parseShort((String) l.get(samplenumber));
						}
						
						else if(typeOfColumn.equalsIgnoreCase("byte")){
							byte bytevara= Byte.parseByte(valuecheck);
							byte bytevarb = Byte.parseByte((String) l.get(samplenumber));
							if(bytevarb<bytevara){
								integerlistarray.add(k);
							}
							flag=1;
						}
						else if(typeOfColumn.equalsIgnoreCase("double")){
							double f= Double.parseDouble(valuecheck);
							double m = Double.parseDouble((String) l.get(samplenumber));
							if(m<f){
								integerlistarray.add(k);
							}
							flag=1;
						}
						else{
							String str="";
							if(valuecheck.contains("\'") || valuecheck.contains("\"")){
								str=valuecheck.substring(1, valuecheck.length()-1);
							}
							else{
								str=valuecheck;
							}
							int p=l.get(samplenumber).toString().toLowerCase().compareTo(str.toLowerCase());
							if(p<0)
							{
								integerlistarray.add(k);
							}
							flag=1;
						}

						if(intvar1<intvar2 && flag==0)
						{
							integerlistarray.add(k);
						}
					}
				}

				for(int t1=0; t1<integerlistarray.size();t1++)
				{
		
					ArrayList resultset = listToOut.get(integerlistarray.get(t1));

					for(int h=0;h<resultset.size();h++)
					{
						System.out.print(resultset.get(h)+"  |  ");
					}
					System.out.println();
				}
			}
			else{
				System.out.println("Invalid");
			}


		} catch(Exception e){
			e.printStackTrace();
		}
	}

	private  ArrayList<ArrayList<String>> getColumnName(String tableName){
		String samplestringa;
		String samplestringb;
		String nameOfColumn = "";
		String type="";
		int sample1=0;
		int sample2=0;
		int number=0;
		
		ArrayList<ArrayList<String>> arr=new ArrayList<ArrayList<String>>();
		ArrayList<String> columnNamesArrayList = new ArrayList<String>();
		ArrayList<String> columnTypeArray = new ArrayList<String>();

		try{
			RandomAccessFile columns = new RandomAccessFile("information_schema.columns.tbl", "rw");
			for(int i=0;i<columns.length();i++){
				samplestringa="";
				sample1=0;
				sample2=0;
				number=0;
				if(columns.getFilePointer()<columns.length()){
					
					for(int k=0;k<3;k++){
						samplestringb="";
						nameOfColumn="";
						byte length= columns.readByte();
						for(int j=0;j<length;j++)
							samplestringb += (char)columns.readByte();
					
						if(nameOfSchema.equalsIgnoreCase(samplestringb)){
					
							sample2 = 1;
						}
						if(tableName.equalsIgnoreCase(samplestringb) && sample2==1){
					
							sample1=1;
						}
						if(k==2 && sample1==1){
					
							nameOfColumn = samplestringb;
						}
					}
					if(sample1==1){
						number = columns.readInt();
					
					
					}
					else{
					
						columns.readInt();
					}
					byte length= columns.readByte();
					for(int j=0;j<length;j++){
						samplestringa += (char)columns.readByte();
					}
					
					if(sample1==1){
					
						type = samplestringa;
						
					}
					byte collen1= columns.readByte();
					for(int j=0;j<collen1;j++)
						columns.readByte();
					byte collen2= columns.readByte();
					if(collen2 !=0){
						
						for(int j=0;j<collen2;j++)
							columns.readByte();
					}
					
					if(sample1==1){
						columnNamesArrayList.add(nameOfColumn);
						columnTypeArray.add(type);
					}
				}
			}
		} catch(Exception e){
			e.printStackTrace();
		}
		arr.add(columnNamesArrayList);
		arr.add(columnTypeArray);
		return arr;
	}

	private void select(String userCommand) {
		ArrayList<ArrayList<String>> arrayList = new ArrayList<ArrayList<String>>();
		ArrayList<String> columnName = new ArrayList<String>();
		ArrayList<String> columnType = new ArrayList<String>();
		String[] usercommandsplit = userCommand.split(" ");
		String nameOfTable =usercommandsplit[3];
		
		try {
			arrayList = getColumnName(nameOfTable);
			columnName=arrayList.get(0);
			columnType=arrayList.get(1);
			displayTable(columnName,columnType,nameOfTable);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void displayTable(ArrayList<String> columnName, ArrayList<String> columnType, String nameOfTable) {
		
		displayLine();
		for(int i=0;i<columnName.size();i++)
			System.out.print(columnName.get(i)+"  |  ");
		System.out.println();
		displayLine();
		

		String stringCoulmnType="";
		try {

			RandomAccessFile tableFile = new RandomAccessFile(nameOfSchema.toLowerCase()+"."+nameOfTable.toLowerCase()+".tbl","rw");
			for(int j=0;j<tableFile.length();j++){
				if(tableFile.getFilePointer()<tableFile.length()){
					for(int k=0;k<columnType.size();k++){
						stringCoulmnType=columnType.get(k);
						if(stringCoulmnType.equalsIgnoreCase("int")){
							System.out.print(tableFile.readInt()+"  |  ");
						}
						else if(stringCoulmnType.equalsIgnoreCase("double")){
							System.out.print(tableFile.readDouble()+"  |  ");
						}
						else if(stringCoulmnType.equalsIgnoreCase("float")){
							System.out.print(tableFile.readFloat()+"  |  ");
						}
						else if(stringCoulmnType.equalsIgnoreCase("long")|| stringCoulmnType.equalsIgnoreCase("long int") ){
							System.out.print(tableFile.readLong()+"  |  ");
						}
						else if(stringCoulmnType.equalsIgnoreCase("short") || stringCoulmnType.equalsIgnoreCase("short int")){
							System.out.print(tableFile.readShort()+"  |  ");
						}
						
						else if(stringCoulmnType.equalsIgnoreCase("byte")){
							System.out.print(tableFile.readByte()+"  |  ");
						}
						else { 
							byte stringlength =tableFile.readByte();
							String stringname="";
							for(int l=0;l<stringlength;l++)
								stringname += (char) tableFile.readByte();
							System.out.print(stringname+"      |      ");
						}
					}
					System.out.println();
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// ** line display
	private void displayLine(){
		for(int i=0;i<10;i++)
			System.out.print("--------");
		System.out.println();
	}

	// ***** Initializing the Tree Map with the Index File *******//
	private static void mapInitialize(String nameOfTable,String nameOfColumn,String columnType){
		String key="";
		int size=0;
		map1.clear();
		try {
			RandomAccessFile indexFile = new RandomAccessFile(nameOfSchema.toLowerCase()+"."+nameOfTable.toLowerCase()+"."+nameOfColumn.toLowerCase()+".ndx","rw");
			for(int m=0;m<indexFile.length();m++){
				if(indexFile.getFilePointer()<indexFile.length()){
					if(columnType.equalsIgnoreCase("int") || columnType.equalsIgnoreCase("long") || columnType.equalsIgnoreCase("short") || columnType.equalsIgnoreCase("double") || columnType.equalsIgnoreCase("float") || columnType.equalsIgnoreCase("byte")){
						ArrayList<Integer> address = new ArrayList<Integer>();
						int value= indexFile.readInt();
						key = Integer.toString(value);
						size =indexFile.readInt();
						for(int j=0;j<size;j++){
							//	System.out.println("key is : "+ key);
							address.add(indexFile.readInt());
						}
						map1.put(key, address);
					}
					else{
						ArrayList<Integer> address1 = new ArrayList<Integer>();
						byte len = indexFile.readByte();
						String str ="";
						for(int i=0;i<len;i++){
							str += (char) indexFile.readByte();
						}
						key =str;
						size =indexFile.readInt();
						for(int j=0;j<size;j++){
							address1.add(indexFile.readInt());
						}
						map1.put(key, address1);
					}
				}
			}
			} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@SuppressWarnings({ "unchecked", "rawtypes", "resource" })
	private static void insert(String usercommand) {
		String[] names=usercommand.split("[(]");
		String[] tableN = names[0].split(" ");
		String nameOfTable = tableN[2];
		String[] value = names[1].split("[,)]");
		ArrayList<ArrayList<String>> colType= new ArrayList<ArrayList<String>>();
		String primarykey = "";
		String notNullCheck = "";
		String[] colName_type=new String[2];
		String[] samptype;
		ArrayList<ArrayList<String>> checkPN= new ArrayList<ArrayList<String>>();
		ArrayList<String> columnName = new ArrayList<String>();
		ArrayList<String> primaryarraylist = new ArrayList<String>();
		ArrayList<String> notNullArrayList = new ArrayList<String>();
		
		
		ArrayList<Integer> address = new ArrayList<Integer>();
		int flag=0;
		try {
			RandomAccessFile tableFile = new RandomAccessFile(nameOfSchema.toLowerCase()+"."+nameOfTable.toLowerCase()+".tbl","rw");
			tableFile.seek(tableFile.length());
			int k=(int) tableFile.getFilePointer();

			checkPN = checkPrimaryNull(nameOfTable);
			columnName=checkPN.get(0);
			primaryarraylist=checkPN.get(3);
			notNullArrayList=checkPN.get(2);

			if(columnName.size()==value.length){
				for(int i=0;i<value.length;i++){
					colName_type =valueType(nameOfTable,i+1);
					mapInitialize(nameOfTable,colName_type[0],colName_type[1]); // Initializing the tree map with the existing indices.
					if(map1.containsKey(value[i])){
						System.out.println(primaryarraylist.get(i));
						if(primaryarraylist.get(i).equalsIgnoreCase("PRI")){
							flag=1;
							break;
						}
						else{
						ArrayList<Integer> address1 = (ArrayList<Integer>) map1.get(value[i]);
						address1.add(k);
						}
					}
					else{
						address = new ArrayList<Integer>();
						address.add(k);
						map1.put(value[i], address);
					}

					// *** Writing in table file *****
					tableFile.seek(tableFile.length());
					samptype=colName_type[1].split(" ");
					if(samptype[0].equalsIgnoreCase("int")){
						tableFile.writeInt(Integer.parseInt(value[i]));
					}
					else if(samptype[0].equalsIgnoreCase("long")){
						tableFile.writeLong(Long.parseLong(value[i]));
					}
					
					else if(samptype[0].equalsIgnoreCase("byte")){
						tableFile.writeByte(Byte.parseByte(value[i]));
					}
					else if(samptype[0].equalsIgnoreCase("short")){
						tableFile.writeShort(Short.parseShort(value[i]));
					}
					else if(samptype[0].equalsIgnoreCase("double")){
						tableFile.writeDouble(Double.parseDouble(value[i]));
					}
					else if(samptype[0].equalsIgnoreCase("float")){
						tableFile.writeFloat(Float.parseFloat(value[i]));
					}
					else { // for varchar, char, dateTime, date
						tableFile.writeByte(value[i].length()-2);
						tableFile.writeBytes(value[i].substring(1, value[i].length()-1));
					}

					// ****** creating index file ****//
					RandomAccessFile indexFile = new RandomAccessFile(nameOfSchema.toLowerCase()+"."+nameOfTable.toLowerCase()+"."+colName_type[0].toLowerCase()+".ndx","rw");
					for(Entry<Object,ArrayList> entry : map1.entrySet()) {
						Object key = entry.getKey();         // Get the index key
						ArrayList value1 = entry.getValue();   // Get the list of record addresses
						if(samptype[0].equalsIgnoreCase("int") || samptype[0].equalsIgnoreCase("long") || samptype[0].equalsIgnoreCase("short") || samptype[0].equalsIgnoreCase("double") || samptype[0].equalsIgnoreCase("float") || samptype[0].equalsIgnoreCase("byte")){
							int size =value1.size();
							indexFile.writeInt(Integer.parseInt(key.toString()));
							indexFile.writeInt(size);
							for(int j=0;j<size;j++){
								indexFile.writeInt((int) value1.get(j));
							}
						}
						else{
							int size =value1.size();
							indexFile.writeByte(key.toString().length());
							indexFile.writeBytes(key.toString());
							indexFile.writeInt(size);
							for(int j=0;j<size;j++){
								indexFile.writeInt((int) value1.get(j));
							}
						}
					}
				}
			}
			if(flag==0){
			incrementRows(nameOfSchema,nameOfTable); //increment number of rows in the table of information_schema
			System.out.println("Row Inserted Successfully.");
			}
			else{
				System.out.println("Duplicate Primary Key");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("resource")
	private static ArrayList<ArrayList<String>> checkPrimaryNull(String tableName){
		ArrayList<ArrayList<String>> arrayList=new ArrayList<ArrayList<String>>();
		ArrayList<String> colName = new ArrayList<String>();
		ArrayList<String> columnTypeArrayList = new ArrayList<String>();
		ArrayList<String> primaryKeyArrayList = new ArrayList<String>();
		String samplestring;
		String samplestring1;
		String nameOfColumn = "";
		String type="";
		int flag1=0;
		int flag=0;
		ArrayList<String> notNullArrayList = new ArrayList<String>();
		try{
			RandomAccessFile columnsTableFile = new RandomAccessFile("information_schema.columns.tbl", "rw");
			for(int i=0;i<columnsTableFile.length();i++){
				samplestring="";
				flag1=0;
				flag=0;
				if(columnsTableFile.getFilePointer()<columnsTableFile.length()){
					//System.out.println(schemataTableFile.getFilePointer()<schemataTableFile.length());
					for(int k=0;k<3;k++){
						samplestring1="";
						nameOfColumn="";
						byte len= columnsTableFile.readByte();
						for(int j=0;j<len;j++)
							samplestring1 += (char)columnsTableFile.readByte();
						//System.out.println("Value of str1: "+ str1);
						if(nameOfSchema.equalsIgnoreCase(samplestring1)){
							//	System.out.println("111111111");
							flag = 1;
						}
						if(tableName.equalsIgnoreCase(samplestring1) && flag==1){
							//System.out.println("222222222");
							flag1=1;
						}
						if(k==2 && flag1==1){
							//System.out.println("3333");
							nameOfColumn = samplestring1;
						}
					}
					if(flag1==1){
						columnsTableFile.readInt();
						//	System.out.println("ordinal position: "+num);
						//	System.out.println("comp "+(num==col));

					}
					else{
						//System.out.println("5");
						columnsTableFile.readInt();
					}
					byte len= columnsTableFile.readByte();
					for(int j=0;j<len;j++){
						samplestring += (char)columnsTableFile.readByte();
					}
					//	System.out.println("Value of str: "+ str);
					if(flag1==1){
						//System.out.println("6");
						type = samplestring;
						//System.out.println(type+ " in flag2");

						byte len1= columnsTableFile.readByte();
						String str2="";
						for(int j=0;j<len1;j++)
							str2 += (char) columnsTableFile.readByte();
						byte len2= columnsTableFile.readByte();
						String str3="";
						if(len2 !=0){
							for(int j=0;j<len2;j++)
								str3 += (char) columnsTableFile.readByte();
						}
						else{
							str3="";
						}
						colName.add(nameOfColumn);
						columnTypeArrayList.add(type);
						notNullArrayList.add(str2);
						primaryKeyArrayList.add(str3);
					}
					else{
						type = samplestring;
						byte len1= columnsTableFile.readByte();
						String str2="";
						for(int j=0;j<len1;j++)
							str2 +=columnsTableFile.readByte();
						byte len2= columnsTableFile.readByte();
						String str3="";
						if(len2 !=0){
							for(int j=0;j<len2;j++)
								str3 +=columnsTableFile.readByte();
						}
						else{
							str3="";
						}

					}
				}
			}
		} catch(Exception e){
			e.printStackTrace();
		}
		arrayList.add(colName);
		arrayList.add(columnTypeArrayList);
		arrayList.add(notNullArrayList);
		arrayList.add(primaryKeyArrayList);
		return arrayList;
	}

	private static  String[] valueType(String tableName, int col){
		String columnName="";
		String sampstr="";
		String datatypeOfValue="";
		String samplestr1="";
		int sampint=0;
		int sampint1=0;
		int sampint2=0;
		int sampnum=0;
		String[] array=new String[2];
		try{
			RandomAccessFile columnsTableFile = new RandomAccessFile("information_schema.columns.tbl", "rw");
			for(int i=0;i<columnsTableFile.length();i++){
				sampstr="";
				sampint2=0;
				sampint1=0;
				sampint=0;
				if(columnsTableFile.getFilePointer()<columnsTableFile.length()){
					//System.out.println(schemataTableFile.getFilePointer()<schemataTableFile.length());
					for(int k=0;k<3;k++){
						samplestr1="";
						columnName="";
						byte len= columnsTableFile.readByte();
						for(int j=0;j<len;j++)
							samplestr1 += (char)columnsTableFile.readByte();
						//System.out.println("Value of str1: "+ str1);
						if(nameOfSchema.equalsIgnoreCase(samplestr1)){
							//	System.out.println("111111111");
							sampint = 1;
						}
						if(tableName.equalsIgnoreCase(samplestr1) && sampint==1){
							//System.out.println("222222222");
							sampint1=1;
						}
						if(k==2 && sampint1==1){
							//System.out.println("3333");
							columnName = samplestr1;
						}
					}
					if(sampint1==1){
						sampnum = columnsTableFile.readInt();
						if(sampnum==col){
							sampint2=1;
						}
					}
					else{
						columnsTableFile.readInt();
					}
					byte len= columnsTableFile.readByte();
					for(int j=0;j<len;j++){
						sampstr += (char)columnsTableFile.readByte();
					}
					
					if(sampint2==1){
						datatypeOfValue = sampstr;
					}
					byte len1= columnsTableFile.readByte();
					for(int j=0;j<len1;j++)
						columnsTableFile.readByte();
					byte len2= columnsTableFile.readByte();
					if(len2 !=0){
					
						for(int j=0;j<len2;j++)
							columnsTableFile.readByte();
					}
					
					if(sampint2==1){
						array[0]=columnName;
						array[1]=datatypeOfValue;
					}
				}
			}
			
		}catch (Exception e) {
			
			e.printStackTrace();
		}

		return array;
	}

	private static void incrementRows(String schema, String tableName){
		int f=0,f1=0;
		long rows=0;
		try{
			RandomAccessFile tablesTableFile = new RandomAccessFile("information_schema.tables.tbl", "rw");
			for(int i=0;i<tablesTableFile.length();i++){
				f=0;
				f1=0;
				if(tablesTableFile.getFilePointer()<tablesTableFile.length()){
					for(int k=0;k<2;k++){
						String str1="";
						byte len= tablesTableFile.readByte();
						for(int j=0;j<len;j++)
							str1 += (char)tablesTableFile.readByte();
						if(schema.equalsIgnoreCase(str1)){
							f = 1;
						}
						if(tableName.equalsIgnoreCase(str1) && f==1){
							f1=1;
						}
					}
					if(f1==1){
						rows = tablesTableFile.readLong();
					}
					else{
						tablesTableFile.readLong();
					}
				}
			}
	
			f=0;
			f1=0;
			RandomAccessFile tablesTableFile1 = new RandomAccessFile("information_schema.tables.tbl", "rw");
			for(int i=0;i<tablesTableFile1.length();i++){
				f=0;
				f1=0;
				if(tablesTableFile1.getFilePointer()<tablesTableFile1.length()){
					
					for(int k=0;k<2;k++){
						String str1="";
						byte len= tablesTableFile1.readByte();
						for(int j=0;j<len;j++)
							str1 += (char)tablesTableFile1.readByte();
						if(schema.equalsIgnoreCase(str1)){
							f = 1;
						}
						if(tableName.equalsIgnoreCase(str1) && f==1){
							f1=1;
						}
					}
					if(f1==1){
					
						rows=rows+1;
						tablesTableFile1.writeLong(rows);
					}
					else
						tablesTableFile1.readLong();
				}
			}
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void showSchemaTables() {
		int f=0;
		String str1 = null;
		String str;
		long len1;
		try {
			RandomAccessFile tablesTableFile = new RandomAccessFile("information_schema.tables.tbl", "rw");
			for(int i=0;i<tablesTableFile.length();i++){
				str1="";
				if(tablesTableFile.getFilePointer()<tablesTableFile.length()){
			
					for(int k=0;k<2;k++){
						str="";
						byte len= tablesTableFile.readByte();
						for(int j=0;j<len;j++){
							str = str+ (char)tablesTableFile.readByte();
							if(f==1){
								str1 = str;
							}
						}
						if(nameOfSchema.equalsIgnoreCase(str)){
							f=1;
						}
						else
							f=0;
						if(!str1.isEmpty()){
							System.out.println(str1);
						}
					}

					tablesTableFile.readLong();
				}
			}


		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void tableCreate(String command){
		String[] tokenizer =null;
		
		
		String[] callstring = null;
		String[] usercommandarray = command.split("[(]");
		String samplestring=usercommandarray[0];
		String sub = command.substring(samplestring.length()+1, command.length()-1);
		
		int flag=0,j=0;
		for(int k= j+1; k<usercommandarray.length; k++)
		{
			tokenizer = sub.split("[,]");
		}
		callstring = new String[tokenizer.length+1];
		callstring[0]=samplestring;
		for(int y=0; y< tokenizer.length ;y++)
		{
			callstring[y+1] = tokenizer[y];
		}
		createTable(callstring);

	}

	private void createTable(String[] strArray)
	{

		int j=0;
		String strLine="";
		String tablefilename=null;
		String tableName= null;
		try {
			RandomAccessFile columnsTableFile = new RandomAccessFile("information_schema.columns.tbl", "rw");
			for (int h=0; h<strArray.length;h++)   {
				strLine = strArray[h];
				String[] tokens = strLine.split(" ");
				for(int i=0; i<tokens.length; i++)
				{

					if(tokens[i].equalsIgnoreCase("TABLE"))
					{
						tableName = tokens[i+1];
						tablefilename = nameOfSchema+"."+tokens[i+1]+".tbl";
						
					}

				}  
				int len=tokens.length;
				if(tokens[0].equals("")){
					for(int u=0;u<tokens.length-1;u++)
					{
						tokens[u]=tokens[u+1];
					}
					len = tokens.length-1;
				}		

				if(j!=0){
					incrementRows("information_schema","columns");
					columnsTableFile.seek(columnsTableFile.length());
					columnsTableFile.writeByte(nameOfSchema.length()); // TABLE_SCHEMA
					columnsTableFile.writeBytes(nameOfSchema);
					columnsTableFile.writeByte(tableName.length()); // TABLE_NAME
					columnsTableFile.writeBytes(tableName);
					columnsTableFile.writeByte(tokens[0].length()); // COLUMN_NAME
					columnsTableFile.writeBytes(tokens[0]);
					columnsTableFile.writeInt(j); // ORDINAL_POSITION
					RandomAccessFile indexFile = new RandomAccessFile(nameOfSchema.toLowerCase()+"."+tableName.toLowerCase()+"."+tokens[0].toLowerCase()+".ndx","rw");
					if(len>2){
						if(tokens[1].equalsIgnoreCase("SHORT") || tokens[1].equalsIgnoreCase("LONG")){
							if(tokens[2].equalsIgnoreCase("INT")){
								String str = tokens[1]+" "+tokens[2];
								columnsTableFile.writeByte(str.length()); // COLUMN_TYPE
								columnsTableFile.writeBytes(str);
								if(len>3){
									if(tokens[3].equalsIgnoreCase("PRIMARY")){
										columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
										columnsTableFile.writeBytes("NO");
										columnsTableFile.writeByte("PRI".length()); // COLUMN_KEY
										columnsTableFile.writeBytes("PRI");
									}

									if(tokens[3].equalsIgnoreCase("NOT")){
										columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
										columnsTableFile.writeBytes("NO");
										columnsTableFile.writeByte("".length()); // COLUMN_KEY
										columnsTableFile.writeBytes("");
									}
								}
								else{
									
									columnsTableFile.writeByte("YES".length()); // IS_NULLABLE
									columnsTableFile.writeBytes("YES");
									columnsTableFile.writeByte("".length()); // COLUMN_KEY
									columnsTableFile.writeBytes("");
								}
							}
							else{
								String str = tokens[1];
								columnsTableFile.writeByte(str.length()); // COLUMN_TYPE
								columnsTableFile.writeBytes(str);
								if(len>2){
									if(tokens[2].equalsIgnoreCase("PRIMARY")){
										columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
										columnsTableFile.writeBytes("NO");
										columnsTableFile.writeByte("PRI".length()); // COLUMN_KEY
										columnsTableFile.writeBytes("PRI");
									}

									if(tokens[2].equalsIgnoreCase("NOT")){
										columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
										columnsTableFile.writeBytes("NO");
										columnsTableFile.writeByte("".length()); // COLUMN_KEY
										columnsTableFile.writeBytes("");
									}
								}
								else{
		
									columnsTableFile.writeByte("YES".length()); // IS_NULLABLE
									columnsTableFile.writeBytes("YES");
									columnsTableFile.writeByte("".length()); // COLUMN_KEY
									columnsTableFile.writeBytes("");
								}
							}
						}

						else{
							columnsTableFile.writeByte(tokens[1].length()); // COLUMN_TYPE
							columnsTableFile.writeBytes(tokens[1]);
							if(tokens[2].equalsIgnoreCase("PRIMARY")){
								columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
								columnsTableFile.writeBytes("NO");
								columnsTableFile.writeByte("PRI".length()); // COLUMN_KEY
								columnsTableFile.writeBytes("PRI");
							}

							if(tokens[2].equalsIgnoreCase("NOT")){
								columnsTableFile.writeByte("YES".length()); // IS_NULLABLE
								columnsTableFile.writeBytes("YES");
								columnsTableFile.writeByte("".length()); // COLUMN_KEY
								columnsTableFile.writeBytes("");
							}
						}
					}

					else{
						
						columnsTableFile.writeByte(tokens[1].length()); // COLUMN_TYPE
						columnsTableFile.writeBytes(tokens[1]);
						columnsTableFile.writeByte("YES".length()); // IS_NULLABLE
						columnsTableFile.writeBytes("YES");
						columnsTableFile.writeByte("".length()); // COLUMN_KEY
						columnsTableFile.writeBytes("");
					}
				}

				j++;
			}
			incrementRows("information_schema","tables");
			tablefilename = nameOfSchema.toLowerCase()+"."+tableName.toLowerCase()+".tbl";
			
			RandomAccessFile newTable = new RandomAccessFile(tablefilename.toLowerCase(),"rw");
			System.out.println("Table Created");
			RandomAccessFile tablesTableFile = new RandomAccessFile("information_schema.tables.tbl", "rw");
			tablesTableFile.seek(tablesTableFile.length());
			tablesTableFile.writeByte(nameOfSchema.length());
			tablesTableFile.writeBytes(nameOfSchema);
			tablesTableFile.writeByte(tableName.length());
			tablesTableFile.writeBytes(tableName);
			tablesTableFile.writeLong(0);
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}


	private void useSchema(String command) {
		// TODO Auto-generated method stub
		RandomAccessFile schemataTableFile;
		String usercommandstring=command;
		int flag=0;
		try {
			if(usercommandstring != null){
				String[] tokens = usercommandstring.split(" ");
				nameOfSchema=tokens[1];
			}
			schemataTableFile = new RandomAccessFile("information_schema.schemata.tbl", "rw");
			for(int i=0;i<schemataTableFile.length();i++){
				String str1="";
				if(schemataTableFile.getFilePointer()<schemataTableFile.length()){
					//System.out.println(schemataTableFile.getFilePointer()<schemataTableFile.length());
					byte length= schemataTableFile.readByte();
					for(int j=0;j<length;j++)
						str1 += (char)schemataTableFile.readByte();
					if(nameOfSchema.equalsIgnoreCase(str1)){
						flag=1;
					}
				}
			}
			if(flag !=1)
				System.out.println("Schema doesnot exist");
			else
				System.out.println("Schema changed");
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}

	

	private void newline(int num) {
		for(int i=0;i<num;i++) {
			System.out.println();
		}
	}

	private void splashScreen() {
		System.out.println(line("*",80));
		System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println(line("*",80));
	}

	
	private  String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	

	
	}

	
}




