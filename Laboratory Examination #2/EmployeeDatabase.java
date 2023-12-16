import java.sql.*;
import com.mysql.cj.util.StringUtils;
import java.util.Scanner;

public class EmployeeDatabase {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/employees";
    private static final String USER = "root";
    private static final String PASSWORD = "";

    public static void main(String[] args) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

            Connection connection = DriverManager.getConnection(DB_URL, USER, PASSWORD);
            Statement statement = connection.createStatement();
            Scanner scanner = new Scanner(System.in);

            while (true) {
                displayMenu();

                int choice = scanner.nextInt();

                switch (choice) {
                    case 1:
                        executeQuery(statement,
                                "SELECT * FROM employees INNER JOIN departments ON employees.emp_no = departments.dept_no");
                        break;
                    case 2:
                        executeQuery(statement,
                                "SELECT * FROM employees LEFT JOIN departments ON employees.emp_no = departments.dept_no");
                        break;
                    case 3:
                        executeQuery(statement,
                                "SELECT * FROM employees RIGHT JOIN departments ON employees.emp_no = departments.dept_no");
                        break;
                    case 4:
                        executeQuery(statement,
                                "SELECT * FROM employees WHERE emp_no IN (SELECT emp_no FROM salaries WHERE salary > 50000)");
                        break;
                    case 5:
                        executeStoredProcedure(connection);
                        break;
                    case 6:
                        executeStoredFunction(connection);
                        break;
                    case 0:
                        statement.close();
                        connection.close();
                        System.exit(0);
                    default:
                        System.out.println("Invalid choice. Please try again.");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void displayMenu() {
        System.out.println("Select an option:");
        System.out.println("1. Join");
        System.out.println("2. Left Join");
        System.out.println("3. Right Join");
        System.out.println("4. Subquery");
        System.out.println("5. Execute Stored Procedure");
        System.out.println("6. Execute Stored Function");
        System.out.println("0. Exit");
    }

    private static void executeQuery(Statement statement, String query) {
        try {
            ResultSet resultSet = statement.executeQuery(query);

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Print column headers
            for (int i = 1; i <= columnCount; i++) {
                System.out.printf("%-20s", metaData.getColumnName(i));
            }
            System.out.println();

            // Print rows
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.printf("%-20s",
                            StringUtils.isNullOrEmpty(resultSet.getString(i)) ? "N/A" : resultSet.getString(i));
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void executeStoredProcedure(Connection connection) {
        try {
            // Create stored procedure
            String storedProcedure = "CREATE PROCEDURE GetTop10EmployeeDetails()\n" +
                    "BEGIN\n" +
                    "    SELECT * FROM employees LIMIT 10;\n" +
                    "END;";

            Statement statement = connection.createStatement();

            try {
                // Try to create the stored procedure
                statement.execute(storedProcedure);
            } catch (SQLSyntaxErrorException syntaxException) {
                // Stored procedure already exists, ignore the exception
                System.out.println("Stored procedure 'GetTop10EmployeeDetails' already exists.");
            }

            // Show the result
            executeQuery(statement, "CALL GetTop10EmployeeDetails()");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void executeStoredFunction(Connection connection) {
        try {
            String storedFunction = "CREATE FUNCTION GetTotalSalary(emp_id INT) RETURNS INT BEGIN DECLARE total_salary INT;"
                    +
                    " SELECT COALESCE(SUM(salary), 0) INTO total_salary FROM salaries WHERE emp_no = emp_id; RETURN total_salary; END;";

            // Declare a new statement
            try (Statement statement = connection.createStatement()) {
                // Try to create the function
                try {
                    statement.execute(storedFunction);
                    System.out.println("Function GetTotalSalary created successfully.");
                } catch (SQLSyntaxErrorException e) {
                    // If the function already exists, proceed with showing the result
                    System.out.println("Function GetTotalSalary already exists.");

                    // Now, execute the function and show the result
                    try (PreparedStatement preparedStatement = connection
                            .prepareStatement("SELECT GetTotalSalary(?) AS total_salary")) {
                        Scanner scanner = new Scanner(System.in);
                        System.out.print("Enter employee ID: ");
                        int empId = scanner.nextInt();

                        preparedStatement.setInt(1, empId);
                        try (ResultSet resultSet = preparedStatement.executeQuery()) {
                            if (resultSet.next()) {
                                int totalSalary = resultSet.getInt("total_salary");
                                System.out.println("Total Salary: " + totalSalary);
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
