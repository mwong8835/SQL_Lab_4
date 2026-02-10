import java.sql.*;
import java.util.Scanner; 

public class example {	
    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost";

    static final String USER = "root";
    static final String PASS = "Root1234.";

    public static void main(String[] args) {
    	Scanner scanner = new Scanner(System.in);

        Connection conn = null;
        Statement stmt = null;
        try{        
            Class.forName("com.mysql.cj.jdbc.Driver");
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            System.out.println("Creating database...");
            stmt = conn.createStatement();

            String sql = "CREATE DATABASE IF NOT EXISTS VehicleOffice";
            stmt.executeUpdate(sql);
            System.out.println("Database created successfully...");

            sql = "use VehicleOffice";
            stmt.executeUpdate(sql);

            //STEP 5.1: Create Table Branch;
            sql = "create table IF NOT EXISTS branch( branch_id integer not null PRIMARY KEY, " +
                "branch_name varchar(20) not null," +
                "branch_addr varchar(50)," +
                "branch_city varchar(20) not null," +
                "branch_phone integer)";
            stmt.executeUpdate(sql);

            //STEP 5.2: Create Driver Table;
            sql = "create table IF NOT EXISTS driver(" +
                  "driver_ssn integer not null PRIMARY KEY, " +
                  "driver_name varchar(20) not null, " +
                  "driver_addr varchar(50), " +
                  "driver_city varchar(20) not null, " +
                  "driver_birthdate date, " +
                  "driver_phone integer)";
            stmt.executeUpdate(sql);
            
            //STEP 5.3: Create License Table;
            sql = "create table IF NOT EXISTS license(" +
                "license_no integer not null PRIMARY KEY, " +
                "driver_ssn integer not null, " +
                "license_type char(1), " +
                "license_class integer, " +
                "license_expiry date, " +
                "issue_date date, " +
                "branch_id integer, " +
                "FOREIGN KEY (driver_ssn) REFERENCES driver(driver_ssn), " +
                "FOREIGN KEY (branch_id) REFERENCES branch(branch_id))";
            stmt.executeUpdate(sql);

            //STEP 5.4: Create Exam Table;
            sql = "create table IF NOT EXISTS exam(" +
                "driver_ssn integer not null, " +
                "branch_id integer not null, " +
                "exam_date date, " +
                "exam_type char(1), " +
                "exam_score integer, " +
                "PRIMARY KEY (driver_ssn, branch_id, exam_date), " +
                "FOREIGN KEY (driver_ssn) REFERENCES driver(driver_ssn), " +
                "FOREIGN KEY (branch_id) REFERENCES branch(branch_id))";
            stmt.executeUpdate(sql);     

            
            
            
            /* -------------------------- COMMENT OUT AFTER 1 RUN BECAUSE THEY CAN ONLY BE INSERTED INTO THE DATABASE ONCE -------------------------- */
            
            
            //STEP 6.1: insert tuples into Branch table;
            sql = "insert into branch values(10, 'Main', '1234 Main St.', 'Hoboken', 5551234)";
            stmt.executeUpdate(sql);
            sql = "insert into branch values(20, 'NYC 33rd street', '2320 33rd street', 'NYC', 5552331)";
            stmt.executeUpdate(sql);
            sql = "insert into branch values(30, 'West Creek', '251 Creek Rd.', 'Newark', 5552511)";
            stmt.executeUpdate(sql);
            sql = "insert into branch values(40, 'Blenheim', '1342 W.22 Ave.', 'Princeton', 5551342)";
            stmt.executeUpdate(sql);
            sql = "insert into branch values(50, 'NYC 98 street', '340 98th street', 'NYC', 5214202)";
            stmt.executeUpdate(sql);
            sql = "insert into branch values(60, 'NYC 4th street', '21 4th street', 'NYC', 5214809)";
            stmt.executeUpdate(sql);
            stmt.executeUpdate(sql);

            //STEP 6.2: insert tuples into the driver table;
            sql = "insert into driver values(11111111, 'Bob Smith', '111 E.11 Street', 'Hoboken', '1975-01-01', 5551111)";
            stmt.executeUpdate(sql);
            sql = "insert into driver values(22222222, 'John Walters', '222 E.22 St.', 'Princeton', '1976-02-02', 5552222)";
            stmt.executeUpdate(sql);
            sql = "insert into driver values(33333333, 'Troy Rops', '333 W 33 Ave', 'NYC', '1970-03-03', 5553333)";
            stmt.executeUpdate(sql);
            sql = "insert into driver values(44444444, 'Kevin Mark', '444 E.4 Ave.', 'Hoboken', '1974-04-04', 5554444)";
            stmt.executeUpdate(sql);
            sql = "insert into driver values(55555555, 'Amelie Kim', '63 Main street', 'Hoboken', '2000-09-10', 5551456)";
            stmt.executeUpdate(sql);
            sql = "insert into driver values(66666666, 'Mary Gup', '47 W 13th street', 'NYC', '1998-12-31', 5552315)";
            stmt.executeUpdate(sql);
            sql = "insert into driver values(77777777, 'Clark Johnson', '36 east 8th street', 'NYC', '1999-10-01', 5559047)";
            stmt.executeUpdate(sql);

            //STEP 6.3: insert tuples into the license table;
            sql = "insert into license values (1, 11111111, 'D', 5, '2022-05-24', '2017-05-25', 20)";
            stmt.executeUpdate(sql);
            sql = "insert into license values (2, 22222222, 'D', 5, '2024-09-29', '2016-08-29', 40)";
            stmt.executeUpdate(sql);
            sql = "insert into license values (3, 33333333, 'L', 5, '2022-12-27', '2017-06-27', 20)";
            stmt.executeUpdate(sql);
            sql = "insert into license values (4, 44444444, 'D', 5, '2022-08-30', '2017-08-30', 40)";
            stmt.executeUpdate(sql);
            sql = "insert into license values (5, 77777777, 'D', 3, '2025-08-17', '2020-08-17', 50)";
            stmt.executeUpdate(sql);
            sql = "insert into license values (6, 66666666, 'D', 1, '2024-01-12', '2020-01-11', 50)";
            stmt.executeUpdate(sql);
            sql = "insert into license values (7, 44444444, 'L', 5, '2023-01-31', '2020-12-31', 30)";
            stmt.executeUpdate(sql);

            //STEP 6.4: insert all tuples into the exam table;
            sql = "insert into exam values (11111111, 20, '2017-05-25', 'D', 79)";
            stmt.executeUpdate(sql);
            sql = "insert into exam values (22222222, 30, '2016-05-06', 'L', 25)";
            stmt.executeUpdate(sql);
            sql = "insert into exam values (22222222, 40, '2016-06-10', 'L', 51)";
            stmt.executeUpdate(sql);
            sql = "insert into exam values (33333333, 10, '2017-07-07', 'L', 45)";
            stmt.executeUpdate(sql);
            sql = "insert into exam values (33333333, 20, '2017-07-27', 'L', 61)";
            stmt.executeUpdate(sql);
            sql = "insert into exam values (44444444, 10, '2017-07-27', 'L', 71)";
            stmt.executeUpdate(sql);
            sql = "insert into exam values (44444444, 20, '2017-08-30', 'L', 65)";
            stmt.executeUpdate(sql);
            sql = "insert into exam values (44444444, 40, '2017-09-01', 'L', 82)";
            stmt.executeUpdate(sql);
            sql = "insert into exam values (11111111, 20, '2017-12-02', 'L', 67)";
            stmt.executeUpdate(sql);
            sql = "insert into exam values (22222222, 40, '2016-08-29', 'D', 81)";
            stmt.executeUpdate(sql);
            sql = "insert into exam values (33333333, 20, '2017-06-27', 'L', 49)";
            stmt.executeUpdate(sql);
            sql = "insert into exam values (44444444, 10, '2019-04-10', 'D', 80)";
            stmt.executeUpdate(sql);
            sql = "insert into exam values (77777777, 30, '2020-12-31', 'L', 90)";
            stmt.executeUpdate(sql);
            sql = "insert into exam values (77777777, 30, '2020-10-30', 'L', 40)";
            stmt.executeUpdate(sql);
            sql = "insert into exam values (66666666, 40, '2020-02-03', 'D', 90)";
            stmt.executeUpdate(sql);

            // STEP 7: Use SQL to ask queries and retrieve data from the tables;
			Statement s = conn.createStatement ();
			s.executeQuery ("SELECT branch_id, branch_name, branch_addr FROM branch");
			ResultSet rs = s.getResultSet ();
			int count = 0;
			while (rs.next ())
			{
			   int idVal = rs.getInt ("branch_id");
			   String nameVal = rs.getString ("branch_name");
			   String addrVal = rs.getString ("branch_addr");
			   System.out.println (
			       "branch id = " + idVal
			       + ", name = " + nameVal
			       + ", address = " + addrVal);
			   ++count;
			}
			rs.close ();
			s.close ();
			System.out.println (count + " rows were retrieved");

            // Your Task 7: Generate the GUI that supports six options (as indicated in the instructions).
            
            
            System.out.println("---------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Choose from the options below. Type 'exit' to exit. You must write in the format >>[number] [optional-param]");
            System.out.println("---------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("1: Given driver name, get license type, issue/expiry date, and name of issuing branch.");
            System.out.println("2: Given driver name, get exam branch name, exam date, and exam score of driver.");
            System.out.println("3: Given branch name, get name, address, city, phone number, and license type of drivers in that branch.");
            System.out.println("4: Given city name, get branch name, address, phone number, and the total number of licenses issued by all branches in this city.");
            System.out.println("5: Get the names and phone numbers of drivers whose licenses have expired for at least three months by Nov 2, 2024.");
            System.out.println("6: Report data errors in the Exam table");
            System.out.println("---------------------------------------------------------------------------------------------------------------------------------");

            String inp = scanner.nextLine(); 
            
            while (!inp.equals("exit")){   
            	int inpNum = 0;
            	String inpParam = "";
            	
            	String inpNumString = inp.substring(0, 1);
            	
            	if(inp.length()>1) {
            		inpParam = inp.substring(2);
            	}
                
            	inpNum = Integer.parseInt(inpNumString);
            	
            	// 1 Bob Smith
                if(inpNum == 1) {
                	if(inpParam.equals("")) {
                		System.out.println("You must include a parameter for this option!");
                	}
                    String driverName = inpParam; 

                    sql = "SELECT l.license_type, l.issue_date, l.license_expiry, b.branch_name " +
                            "FROM driver d " +
                            "JOIN license l ON d.driver_ssn = l.driver_ssn " +
                            "JOIN branch b ON l.branch_id = b.branch_id " +
                            "WHERE d.driver_name = ?";
                    
                    // PreparedStatement: used to execute parameterized SQL queries
                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setString(1, driverName);
                    ResultSet rs = ps.executeQuery();

                    while (rs.next()) {
                        String licenseType = rs.getString("license_type");
                        Date issueDate = rs.getDate("issue_date");
                        Date licenseExpiry = rs.getDate("license_expiry");
                        String branchName = rs.getString("branch_name");

                        System.out.println("License Type = " + licenseType +
                                ", Issue Date = " + issueDate +
                                ", License Expiry = " + licenseExpiry +
                                ", Branch Name = " + branchName);
                    }

                    rs.close();
                    ps.close();
                }
                else if(inpNum == 2) {
                	if(inpParam.equals("")) {
                		System.out.println("You must include a parameter for this option!");
                	}
                	String driverName = inpParam; 
                	sql = "SELECT b.branch_name, e.exam_date, e.exam_score " +
                            "FROM driver d " +
                            "JOIN exam e ON d.driver_ssn = e.driver_ssn " +
                            "JOIN branch b ON e.branch_id = b.branch_id " +
                            "WHERE d.driver_name = ?";

	               PreparedStatement ps = conn.prepareStatement(sql);
	               ps.setString(1, driverName);
	               ResultSet rs = ps.executeQuery();
	
	               while (rs.next()) {
	                   String branchName = rs.getString("branch_name");
	                   Date examDate = rs.getDate("exam_date");
	                   int examScore = rs.getInt("exam_score");
	
	                   System.out.println("Branch Name = " + branchName +
	                                      ", Exam Date = " + examDate +
	                                      ", Exam Score = " + examScore);
	               }
	
	               rs.close();
	               ps.close();
                	
                	
                }
                else if(inpNum == 3) {
                	if(inpParam.equals("")) {
                		System.out.println("You must include a parameter for this option!");
                	}
                	String branchName = inpParam;
                    sql = "SELECT d.driver_name, d.driver_addr, d.driver_city, d.driver_phone, l.license_type " +
                                 "FROM driver d " +
                                 "JOIN license l ON d.driver_ssn = l.driver_ssn " +
                                 "JOIN branch b ON l.branch_id = b.branch_id " +
                                 "WHERE b.branch_name = ?";

                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setString(1, branchName);
                    ResultSet rs = ps.executeQuery();

                    while (rs.next()) {
                        String driverName = rs.getString("driver_name");
                        String driverAddr = rs.getString("driver_addr");
                        String driverCity = rs.getString("driver_city");
                        int driverPhone = rs.getInt("driver_phone");
                        String licenseType = rs.getString("license_type");

                        System.out.println("Driver Name = " + driverName +
                                           ", Address = " + driverAddr +
                                           ", City = " + driverCity +
                                           ", Phone = " + driverPhone +
                                           ", License Type = " + licenseType);
                    }

                    rs.close();
                    ps.close();
                	
                	
                	
                }
                else if(inpNum == 4) {
                	if(inpParam.equals("")) {
                		System.out.println("You must include a parameter for this option!");
                	}
                	
                	String cityName = inpParam;
                    sql = "SELECT b.branch_name, b.branch_addr, b.branch_phone, COUNT(l.license_no) AS total_licenses " +
                                 "FROM branch b " +
                                 "LEFT JOIN license l ON b.branch_id = l.branch_id " +
                                 "WHERE b.branch_city = ? " +
                                 "GROUP BY b.branch_name, b.branch_addr, b.branch_phone";

                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setString(1, cityName);
                    ResultSet rs = ps.executeQuery();

                    while (rs.next()) {
                        String branchName = rs.getString("branch_name");
                        String branchAddr = rs.getString("branch_addr");
                        int branchPhone = rs.getInt("branch_phone");
                        int totalLicenses = rs.getInt("total_licenses");

                        System.out.println("Branch Name = " + branchName +
                                           ", Address = " + branchAddr +
                                           ", Phone = " + branchPhone +
                                           ", Total Licenses Issued = " + totalLicenses);
                    }

                    rs.close();
                    ps.close();
                }
                else if(inpNum == 5) {
                	String dateThreshold = "2024-08-02";
                    sql = "SELECT d.driver_name, d.driver_phone " +
                                 "FROM driver d " +
                                 "JOIN license l ON d.driver_ssn = l.driver_ssn " +
                                 "WHERE l.license_expiry < ?";

                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setDate(1, Date.valueOf(dateThreshold));
                    ResultSet rs = ps.executeQuery();

                    while (rs.next()) {
                        String driverName = rs.getString("driver_name");
                        int driverPhone = rs.getInt("driver_phone");

                        System.out.println("Driver Name = " + driverName +
                                           ", Phone = " + driverPhone);
                    }

                    rs.close();
                    ps.close();
                }
                else if(inpNum == 6) {
                	// Type I Error: Unmatching branch IDs
                    String typeIErrorSQL = "SELECT d.driver_name " +
                                           "FROM driver d " +
                                           "JOIN license l ON d.driver_ssn = l.driver_ssn " +
                                           "LEFT JOIN exam e ON l.driver_ssn = e.driver_ssn " +
                                           "WHERE l.branch_id != e.branch_id AND e.branch_id IS NOT NULL";
                    ResultSet rs1 = stmt.executeQuery(typeIErrorSQL);
                    System.out.println("Type I Error – Unmatching branch IDs:");
                    while (rs1.next()) {
                        String driverName = rs1.getString("driver_name");
                        System.out.println(driverName);
                    }
                    rs1.close();

                    // Type II Error: Unmatching issue date
                    String typeIIErrorSQL = "SELECT d.driver_name " +
                                            "FROM driver d " +
                                            "JOIN license l ON d.driver_ssn = l.driver_ssn " +
                                            "JOIN (SELECT driver_ssn, MAX(exam_date) AS latest_exam_date FROM exam GROUP BY driver_ssn) e " +
                                            "ON d.driver_ssn = e.driver_ssn " +
                                            "WHERE l.issue_date < e.latest_exam_date";
                    ResultSet rs2 = stmt.executeQuery(typeIIErrorSQL);
                    System.out.println("Type II Error – Unmatching issue date:");
                    while (rs2.next()) {
                        String driverName = rs2.getString("driver_name");
                        System.out.println(driverName);
                    }
                    rs2.close();

                    // Type III Error: Unmatching license type
                    String typeIIIErrorSQL = "SELECT d.driver_name " +
                                             "FROM driver d " +
                                             "JOIN license l ON d.driver_ssn = l.driver_ssn " +
                                             "JOIN (SELECT driver_ssn, exam_type, MAX(exam_date) AS latest_exam_date FROM exam GROUP BY driver_ssn, exam_type) e " +
                                             "ON d.driver_ssn = e.driver_ssn " +
                                             "WHERE l.license_type != e.exam_type";
                    ResultSet rs3 = stmt.executeQuery(typeIIIErrorSQL);
                    System.out.println("Type III Error – Unmatching license type:");
                    while (rs3.next()) {
                        String driverName = rs3.getString("driver_name");
                        System.out.println(driverName);
                    }
                    rs3.close();

                    // Type IV Error: Invalid exam score
                    String typeIVErrorSQL = "SELECT d.driver_name " +
                                            "FROM driver d " +
                                            "JOIN license l ON d.driver_ssn = l.driver_ssn " +
                                            "JOIN (SELECT driver_ssn, MAX(exam_score) AS highest_exam_score FROM exam GROUP BY driver_ssn) e " +
                                            "ON d.driver_ssn = e.driver_ssn " +
                                            "WHERE e.highest_exam_score <= 70";
                    ResultSet rs4 = stmt.executeQuery(typeIVErrorSQL);
                    System.out.println("Type IV Error – Invalid exam score:");
                    while (rs4.next()) {
                        String driverName = rs4.getString("driver_name");
                        System.out.println(driverName);
                    }
                    rs4.close();

                    // Type V Error: No exam
                    String typeVErrorSQL = "SELECT d.driver_name " +
                                           "FROM driver d " +
                                           "JOIN license l ON d.driver_ssn = l.driver_ssn " +
                                           "LEFT JOIN exam e ON d.driver_ssn = e.driver_ssn " +
                                           "WHERE e.driver_ssn IS NULL";
                    ResultSet rs5 = stmt.executeQuery(typeVErrorSQL);
                    System.out.println("Type V Error – No exam:");
                    while (rs5.next()) {
                        String driverName = rs5.getString("driver_name");
                        System.out.println(driverName);
                    }
                    rs5.close();
                }
                else {
                    System.out.println("///////////////////////\n//Not a valid option!//\n///////////////////////");
                }
                
                
                System.out.println("---------------------------------------------------------------------------------------------------------------------------------");
                System.out.println("Choose from the options below. Type 'exit' to exit. You must write in the format >>[number] [optional-param]");
                System.out.println("---------------------------------------------------------------------------------------------------------------------------------");
                System.out.println("1: Given driver name, get license type, issue/expiry date, and name of issuing branch.");
                System.out.println("2: Given driver name, get exam branch name, exam date, and exam score of driver.");
                System.out.println("3: Given branch name, get name, address, city, phone number, and license type of drivers in that branch.");
                System.out.println("4: Given city name, get branch name, address, phone number, and the total number of licenses issued by all branches in this city.");
                System.out.println("5: Get the names and phone numbers of drivers whose licenses have expired for at least three months by Nov 2, 2024.");
                System.out.println("6: Report data errors in the Exam table");
                System.out.println("---------------------------------------------------------------------------------------------------------------------------------");
                inp = scanner.nextLine(); 
            }
            
            
            
        }
        catch(SQLException se){
            //Handle errors for JDBC
            se.printStackTrace();
        }
        catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
        }
        finally{
            //finally block used to close resources
            try{
                if(stmt!=null)
                stmt.close();
            }
            catch(SQLException se2){

            }// nothing we can do
            try{
                if(conn!=null)
                conn.close();
            }
            catch(SQLException se){
                se.printStackTrace();
            }//end finally try
        }//end try

        System.out.println("Goodbye!");
        }//end main
    
    
    
    }//end JDBCExample
