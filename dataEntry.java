/* This program builds a database, and inserts data into the tables using embedded mysql.
 * A connection is established to mysql server hosting on a local host: 3306.
 * The user is prompt for their username and password.
 * A database called sports is created, and then use sports.
 * Six tables are created: players, teams, members, tournaments, matches, and earnings.
 * Data is read from csv files and inserted into the corresponding tables.
 * 
 *  Name: Joshua Johnston
 *  Date: 10/20/2017
 * 
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Scanner;

public class dataEntry {
	
	public static void main(String [] args) throws Exception{
		
		Connection con = getConnection();              //Establishing connection with mysql server
		createDataBase(con);                           //Creating the database sports
		selectDataBase(con);                           //Using the datebase sports
		createTables(con);                             //Creating the six tables
		insertData(con);                               //Inserting the data into the six tables
		con.close();                                   //closing the connection to the mysql server
		System.out.println("Exiting program...");		
		
	
	}// end of main()
//-----------------------------------------------------------------------------------------------------------
	 public static Connection getConnection() throws Exception{
		  
		 Scanner input = new Scanner(System.in);
		  
		 try{
			  String driver = "com.mysql.jdbc.Driver";
			  String url = "jdbc:mysql://localhost:3306/";
			  String username;
			  String password;
			  
			  System.out.println("Enter your username: ");     //prompting the user for their username and password
			  username = input.next();
			  System.out.println("Enter your password: ");			  
			  password = input.next();			  
			  input.close();
			  
			  Class.forName(driver);
		   
			  Connection conn = DriverManager.getConnection(url,username,password);   //getting connected with the mysql server
			  System.out.println("Connected to " + url);
			  return conn;
			  
		  	} catch(Exception e){System.out.println(e);}
		  
		  
		  return null;		 
	 } // end of connection()
//-----------------------------------------------------------------------------------------------------------	 
	 public static void createDataBase(Connection con){
		 
		 try {
			 
			PreparedStatement pstmt = con.prepareStatement("CREATE DATABASE sports");
			pstmt.executeUpdate();
					                     
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 finally{
			 System.out.println("Database sports is created...");
		 }
		 
	 } //end of createDataBase()
//-----------------------------------------------------------------------------------------------------------	 
	 public static void selectDataBase(Connection con){
		 
		 try {
			 
				PreparedStatement pstmt = con.prepareStatement("USE sports");
				pstmt.executeUpdate();
						                     
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			 finally{
				 System.out.println("Database changed to sports...");
			 }
		 
	 } //end of selectDataBase()
//-----------------------------------------------------------------------------------------------------------	 
	 public static void createTables(Connection con){
		 
		createTablePlayers(con);
		createTableTeams(con);
		createTableMembers(con);
		createTableTournaments(con);
		createTableMatches(con);
		createTableEarnings(con);
		 
	 } //end of createTables()
//-----------------------------------------------------------------------------------------------------------	 
	 public static void createTablePlayers(Connection con){
		 
		 try {			
			
			PreparedStatement pstmt = con.prepareStatement("CREATE TABLE players(player_id INT NOT NULL AUTO_INCREMENT, "
					                                        + " tag VARCHAR(255), "
					                                        + " real_name VARCHAR(255), "
					                                        + " nationality VARCHAR(255), "
					                                        + " birthday VARCHAR(255), "
					                                        + " game_race VARCHAR(255), "
					                                        + " PRIMARY KEY (player_id))");
			pstmt.executeUpdate();
					                     
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 finally{
			 System.out.println("Table players is created...");
		 }
		 
	 } //end of createTablePlayers()
//-----------------------------------------------------------------------------------------------------------	 
	 public static void createTableTeams(Connection con){
		 
		 try {			
			
			PreparedStatement pstmt = con.prepareStatement("CREATE TABLE teams(team_id INT NOT NULL AUTO_INCREMENT, "
										+ " name VARCHAR(255), "
										+ " founded VARCHAR(255), "
										+ " disbanded VARCHAR(255), "
										+ " PRIMARY KEY (team_id))");
			pstmt.executeUpdate();
					                     
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 finally{
			 System.out.println("Table teams is created...");
		 }
		 
	 } //end of createTableTeams()
//-----------------------------------------------------------------------------------------------------------	 
	 public static void createTableMembers(Connection con){
		 
		 try {			
			
			PreparedStatement pstmt = con.prepareStatement("CREATE TABLE members(player INT NOT NULL, "
										+ " team int, "
										+ " start_date VARCHAR(255) NOT NULL, "
										+ " end_date VARCHAR(255), "															
										+ " PRIMARY KEY (player, start_date))");
			pstmt.executeUpdate();
					                     
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 finally{
			 System.out.println("Table members is created...");
		 }
		 
	 } //end of createTableMembers()
//-----------------------------------------------------------------------------------------------------------	 
	 public static void createTableTournaments(Connection con){
		 
		 try {			
			
			PreparedStatement pstmt = con.prepareStatement("CREATE TABLE tournaments(tournament_id INT NOT NULL, "
										+ " name VARCHAR(255), "
										+ " region VARCHAR(255), "
										+ " major VARCHAR(255), "
										+ " PRIMARY KEY (tournament_id))");
			pstmt.executeUpdate();
					                     
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 finally{
			 System.out.println("Table tournaments is created...");
		 }
		 
	 } // end of createTableTournaments()
//-----------------------------------------------------------------------------------------------------------	 
	 public static void createTableMatches(Connection con){
		 
		 try {			
			
			PreparedStatement pstmt = con.prepareStatement("CREATE TABLE matches(match_id INT NOT NULL AUTO_INCREMENT, "
										+ " date VARCHAR(255), "
										+ " tournament int, "
										+ " playerA int, "
										+ " playerB int, "
										+ " scoreA int, "
										+ " scoreB int, "
										+ " offline VARCHAR(255),  "
										+ " PRIMARY KEY (match_id))");
			pstmt.executeUpdate();
					                     
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 finally{
			 System.out.println("Table matches is created...");
		 }
		 
	 } // end of createTableMatches()
//----------------------------------------------------------------------------------------------------------- 
	 public static void createTableEarnings(Connection con){
		 
		 try {			
			
			PreparedStatement pstmt = con.prepareStatement("CREATE TABLE earnings(tournament INT NOT NULL, "
										+ " player int NOT NULL, "
										+ " prize_money int, "
										+ " position int, "
										+ " PRIMARY KEY (tournament, player) "
										+ " )");
			pstmt.executeUpdate();
					                     
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 finally{
			 System.out.println("Table earnings is created...");
		 }
		 
	 } // end of createTableEarnings()
//----------------------------------------------------------------------------------------------------------- 
	 public static void insertData(Connection con) throws IOException, SQLException{
		 
		insertPlayers(con);
		insertTeams(con);
		insertMembers(con);
		insertTournaments(con);
		insertMatches(con);
		insertEarnings(con);
		 
	 } //end of insertData()
//-----------------------------------------------------------------------------------------------------------	 
	 public static void insertPlayers(Connection con) throws IOException, SQLException{
		 
		 FileReader fo = new FileReader("players.csv");
		 BufferedReader br = new BufferedReader(fo);
		 String currentLine = null;
		 String[] splitWords;
		 int count= 0;
		 
		 currentLine = br.readLine();                  //getting first line from file becasue the player_id is auto-incremented
		 splitWords = currentLine.split(",");
		 
		 long start = System.currentTimeMillis();       //starting time
		 
		 PreparedStatement pstmt = con.prepareStatement("INSERT INTO players "             
					+ " (player_id, tag, real_name, nationality, birthday, game_race)"
					+ " VALUES "
					+ " ( " + splitWords[0] + " , " + splitWords[1] + " , " + splitWords[2]
					+ " , " + splitWords[3] + " , " + splitWords[4] + " , " + splitWords[5] + " ) ");		
		
		 pstmt.executeUpdate();
		 count++;
		 
		 while((currentLine = br.readLine()) != null){
			 
			 	 splitWords = currentLine.split(",");
			 	
			 	try {			
					
					 pstmt = con.prepareStatement("INSERT INTO players "		        //removed the player_id															
							 + " ( tag, real_name, nationality, birthday, game_race)"																	
							 + " VALUES "																	
							 + " ( " + splitWords[1] + " , " + splitWords[2]																	
							 + " , " + splitWords[3] + " , " + splitWords[4] + " , " + splitWords[5] + " ) ");		
					pstmt.executeUpdate();
					count++;		                     
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}			 	
			 	
		 }
		 
		 long end = System.currentTimeMillis();    //stopping the time
		 
		 System.out.println("Inserting data into table players...");
		 System.out.println("Inserted " + count + " rows of data into table players in " + ((end - start) / 1000) + " seconds...");
		 
		 br.close();
		 fo.close();
	 } // end of insertPlayers()
//----------------------------------------------------------------------------------------------------------- 
	 public static void insertTeams(Connection con) throws IOException, SQLException{
		 
		 FileReader fo = new FileReader("teams.csv");
		 BufferedReader br = new BufferedReader(fo);
		 String currentLine = null;
		 String[] splitWords;
		 int count= 0;
		 
		 currentLine = br.readLine();
		 splitWords = currentLine.split(",");
		 
		 long start = System.currentTimeMillis();    //starting time
		 
		 /*
		  * Disbanded can be null.
		  * Checking to see if there is less then 4 words which would mean disbanded is null.
		  * 
		  */
		 if(splitWords.length < 4){                                                   
			 PreparedStatement pstmt = con.prepareStatement("INSERT INTO teams "      
					 + " (team_id, name, founded, disbanded)"
					 + " VALUES "
					 + " ( " + splitWords[0] + " , " + splitWords[1] + " , " + splitWords[2]
					 + " , " + null + " ) ");
			 pstmt.executeUpdate();
			 
		 }
		 else{
			 	PreparedStatement pstmt = con.prepareStatement("INSERT INTO teams "
					 + " (team_id, name, founded, disbanded)"
					 + " VALUES "
					 + " ( " + splitWords[0] + " , " + splitWords[1] + " , " + splitWords[2]
					 + " , " + splitWords[3] + " ) ");
			 	pstmt.executeUpdate();
			 
		 }
		 
		 count++;
		 while((currentLine = br.readLine()) != null){
			 
			 	 splitWords = currentLine.split(",");
			 	
			 	try {			
					
			 		if(splitWords.length < 4){
						 PreparedStatement pstmt = con.prepareStatement("INSERT INTO teams "
								 + " ( name, founded, disbanded)"
								 + " VALUES "
								 + " ( " + splitWords[1] + " , " + splitWords[2]
								 + " , " + null + " ) ");
						 pstmt.executeUpdate();
						 
					 }
					 else{
						 	PreparedStatement pstmt = con.prepareStatement("INSERT INTO teams "
								 + " ( name, founded, disbanded)"
								 + " VALUES "
								 + " ( " + splitWords[1] + " , " + splitWords[2]
								 + " , " + splitWords[3] + " ) ");
						 	pstmt.executeUpdate();
						 
					 }
					count++;		                     
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}			 	
			 	
		 }
		 long end = System.currentTimeMillis(); //stopping the time
		 
		 System.out.println("Inserting data into table teams...");
		 System.out.println("Inserted " + count + " rows of data into table teams in " + ((end - start) / 1000) + " seconds...");
		 
		 br.close();
		 fo.close();
	 } // end of insertTeams()
//-----------------------------------------------------------------------------------------------------------
 public static void insertMembers(Connection con) throws IOException, SQLException{
	 
	 FileReader fo = new FileReader("members.csv");
	 BufferedReader br = new BufferedReader(fo);
	 String currentLine = null;
	 String[] splitWords;
	 int count= 0;
	 
	 currentLine = br.readLine();
	 splitWords = currentLine.split(",");
	 
	 long start = System.currentTimeMillis(); //starting time
	 
	 if(splitWords.length < 4){
		 PreparedStatement pstmt = con.prepareStatement("INSERT INTO members "
				 + " (player, team, start_date, end_date)"
				 + " VALUES "
				 + " ( " + splitWords[0] + " , " + splitWords[1] + " , " + splitWords[2]
				 + " , " + null + " ) ");
		 pstmt.executeUpdate();
		 
	 }
	 else{
		 	PreparedStatement pstmt = con.prepareStatement("INSERT INTO members "
				 + " (player, team, start_date, end_date)"
				 + " VALUES "
				 + " ( " + splitWords[0] + " , " + splitWords[1] + " , " + splitWords[2]
				 + " , " + splitWords[3] + " ) ");
		 	pstmt.executeUpdate();
		 
	 }
	 
	 count++;
	 while((currentLine = br.readLine()) != null){
		 
		 	 splitWords = currentLine.split(",");
		 	
		 	try {			
				
		 		if(splitWords.length < 4){
		 			 PreparedStatement pstmt = con.prepareStatement("INSERT INTO members "
		 					 + " (player, team, start_date, end_date)"
		 					 + " VALUES "
		 					 + " ( " + splitWords[0] + " , " + splitWords[1] + " , " + splitWords[2]
		 					 + " , " + null + " ) ");
		 			 pstmt.executeUpdate();
		 			 
		 		 }
		 		 else{
		 			 	PreparedStatement pstmt = con.prepareStatement("INSERT INTO members "
		 					 + " (player, team, start_date, end_date)"
		 					 + " VALUES "
		 					 + " ( " + splitWords[0] + " , " + splitWords[1] + " , " + splitWords[2]
		 					 + " , " + splitWords[3] + " ) ");
		 			 	pstmt.executeUpdate();
		 			 
		 		 }
				count++;		                     
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			 	
		 	
	 }
	 
	 long end = System.currentTimeMillis(); //ending time
	 
	 System.out.println("Inserting data into table members...");
	 System.out.println("Inserted " + count + " rows of data into table members in " + ( (end-start) / 1000) + " seconds...");
	 
	 br.close();
	 fo.close();
 } //end of insertMembers()
//----------------------------------------------------------------------------------------------------------- 
 public static void insertTournaments(Connection con) throws IOException, SQLException{
	 
	 FileReader fo = new FileReader("tournaments.csv");
	 BufferedReader br = new BufferedReader(fo);
	 String currentLine = null;
	 String[] splitWords;
	 int count= 0;	 
	 
	 long start = System.currentTimeMillis(); //starting time
	 
	 while((currentLine = br.readLine()) != null){
		 
		 	 splitWords = currentLine.split(",");
		 	
		 	try {			
				
		 		if(!(splitWords[2].equals(""))){
		 			 PreparedStatement pstmt = con.prepareStatement("INSERT INTO tournaments "
		 					 + " (tournament_id , name, region, major)"
		 					 + " VALUES "
		 					 + " ( " + splitWords[0] + " , " + splitWords[1] + " , " + splitWords[2]
		 					 + " , " + splitWords[3] + " ) ");
		 			 pstmt.executeUpdate();
		 			 
		 		 }
		 		 else{
		 			 	PreparedStatement pstmt = con.prepareStatement("INSERT INTO tournaments "
		 			 		 + " (tournament_id , name, region, major)"
		 					 + " VALUES "
		 					 + " ( " + splitWords[0] + " , " + splitWords[1] + " , " + null
		 					 + " , " + splitWords[3] + " ) ");
		 			 	pstmt.executeUpdate();
		 			 
		 		 }
		 		count++;		                     
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			 	
		 	
	 }
	 
	 long end = System.currentTimeMillis(); //ending time
	 
	 System.out.println("Inserting data into table tournaments...");
	 System.out.println("Inserted " + count + " rows of data into table tournaments in " + ((end-start) /1000) + " seconds...");
	 
	 br.close();
	 fo.close();
 } //end of insertTournaments()
 //-----------------------------------------------------------------------------------------------------------  
 public static void insertMatches(Connection con) throws IOException, SQLException{
	 
	 FileReader fo = new FileReader("matches_v2.csv");
	 BufferedReader br = new BufferedReader(fo);
	 String currentLine = null;
	 String[] splitWords;
	 int count= 0;
	 
	 currentLine = br.readLine();
	 splitWords = currentLine.split(",");
	 
	 long start = System.currentTimeMillis(); //starting time
	 
	 PreparedStatement pstmt = con.prepareStatement("INSERT INTO matches "
				+ " (match_id, date, tournament, playerA, playerB, scoreA, scoreB, offline)"
				+ " VALUES "
				+ " ( " + splitWords[0] + " , " + splitWords[1] + " , " + splitWords[2]
				+ " , " + splitWords[3] + " , " + splitWords[4] + " , " + splitWords[5] 
				+ " , " + splitWords[6] + " , " + splitWords[7] + " ) ");		
	
	 pstmt.executeUpdate();
	 count++;
	 while((currentLine = br.readLine()) != null){
		 
		 	 splitWords = currentLine.split(",");
		 	
		 	try {			
				
				 pstmt = con.prepareStatement("INSERT INTO matches "
						 + " ( date, tournament, playerA, playerB, scoreA, scoreB, offline)"
						 + " VALUES "
						 + " ( " + splitWords[1] + " , " + splitWords[2]
						 + " , " + splitWords[3] + " , " + splitWords[4] + " , " + splitWords[5] 
						 + " , " + splitWords[6] + " , " + splitWords[7] +  " ) ");
				pstmt.executeUpdate();
				count++;		                     
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			 	
		 	
	 }
	 long end = System.currentTimeMillis(); //ending time
	 
	 System.out.println("Inserting data into table matches...");
	 System.out.println("Inserted " + count + " rows of data into table matches in " + ((end-start) / 1000) + " seconds...");
	 
	 br.close();
	 fo.close();
 } //end of insertMatches()
//----------------------------------------------------------------------------------------------------------- 
 public static void insertEarnings(Connection con) throws IOException, SQLException{
	 
	 FileReader fo = new FileReader("earnings.csv");
	 BufferedReader br = new BufferedReader(fo);
	 String currentLine = null;
	 String[] splitWords;
	 int count= 0;	 
	 PreparedStatement pstmt;
	 
	 long start = System.currentTimeMillis(); //starting time
	 
	 while((currentLine = br.readLine()) != null){
		 
		 	 splitWords = currentLine.split(",");
		 	
		 	try {			
				
				 pstmt = con.prepareStatement("INSERT INTO earnings "											
						 + " ( tournament, player, prize_money, position )"																
						 + " VALUES "																
						 + " ( " + splitWords[0] + " , " + splitWords[1]																
						 + " , " + splitWords[2] + " , " + splitWords[3] +  " ) ");
				 
				pstmt.executeUpdate();
				count++;		                     
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			 	
		 	
	 }
	 
	 long end = System.currentTimeMillis(); //ending time
	 
	 System.out.println("Inserting data into table earnings...");
	 System.out.println("Inserted " + count + " rows of data into table earnings in " + ((end-start) / 1000) + " seconds...");
	 
	 br.close();
	 fo.close();
 } //end of insertEarnings()
//-----------------------------------------------------------------------------------------------------------

} // end of dataEntry Class
/////////////////////////////////////////////////////////////////////////////////////////////////////////////
