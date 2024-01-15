
module Crudop

  ##
  #This module os responsible for the correct dynamo table 
  class Dynamodb
    module Tables

      ## The method for retrieving the employee dynamo table
      def employee_table
        table_environment("employee")
      end

      def table_environment(table_name)
        env_suffix = ENV["TEST"] == "true"? "development" : ENV["TARGET_ENVIRONMENT"]
        "#{table_name}_#{env_suffix}"
      end


      def targets_schema
        {
          "EMPNO" => "Fixnum",
          "FIRSTNAME" => "String",
          "LASTNAME" => "String",
          "WORKDEPT" => "String",
          "PHONENO" => "String",
          "HIREDATE" => "String", 
          "JOB" => "String",
          "EDLEVEL" => "String", 
          "SEX" => "String",
          "BIRTHDATE" => "String", 
          "SALARY" => "Float", 
          "BONUS" => "Float"
        }
      end

      
    end
  end
end