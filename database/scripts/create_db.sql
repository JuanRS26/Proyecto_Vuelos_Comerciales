DROP DATABASE IF EXISTS flights;
CREATE DATABASE flights;
USE flights;


DROP TABLE IF EXISTS `airline`;
CREATE TABLE IF NOT EXISTS `airline`(`Code_IATA` VARCHAR(30),
									  `Airline` VARCHAR(100),
                                      PRIMARY KEY (`Code_IATA`)
);

DROP TABLE IF EXISTS `airplane`;
CREATE TABLE IF NOT EXISTS `airplane`(`tailnum` VARCHAR(50),
  									   `type` VARCHAR(50),
									   `manufacturer` VARCHAR(50),
									   `issue_date` VARCHAR(50),
									   `model` VARCHAR(50),
									   `status` VARCHAR(50),
									   `aircraft_type` VARCHAR(50),
									   `engine_type` VARCHAR(50),
									   `year` INT,
                                       PRIMARY KEY (`tailnum`)
);

DROP TABLE IF EXISTS `airport`;
CREATE TABLE IF NOT EXISTS `airport`(`Code_IATA` VARCHAR(30),
									  `Airport` VARCHAR(100),
									  `City` VARCHAR(100),
									  `State` VARCHAR(100),
									  `Country` VARCHAR(100),
									  `Latitude` FLOAT(10),
									  `Longitude` FLOAT(10),
                                      PRIMARY KEY (`Code_IATA`)
);

DROP TABLE IF EXISTS `y_1987`;
CREATE TABLE IF NOT EXISTS `y_1987`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` INT,
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);


DROP TABLE IF EXISTS `y_1988`;
CREATE TABLE IF NOT EXISTS `y_1988`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` INT,
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_1989`;
CREATE TABLE IF NOT EXISTS `y_1989`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` INT,
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_1990`;
CREATE TABLE IF NOT EXISTS `y_1990`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` INT,
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_1991`;
CREATE TABLE IF NOT EXISTS `y_1991`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` INT,
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_1992`;
CREATE TABLE IF NOT EXISTS `y_1992`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` INT,
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_1993`;
CREATE TABLE IF NOT EXISTS `y_1993`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` INT,
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_1994`;
CREATE TABLE IF NOT EXISTS `y_1994`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` INT,
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_1995`;
CREATE TABLE IF NOT EXISTS `y_1995`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` VARCHAR(50),
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_1996`;
CREATE TABLE IF NOT EXISTS `y_1996`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` VARCHAR(50),
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_1997`;
CREATE TABLE IF NOT EXISTS `y_1997`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` VARCHAR(50),
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_1998`;
CREATE TABLE IF NOT EXISTS `y_1998`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` VARCHAR(50),
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_1999`;
CREATE TABLE IF NOT EXISTS `y_1999`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` VARCHAR(50),
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_2000`;
CREATE TABLE IF NOT EXISTS `y_2000`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` VARCHAR(50),
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_2001`;
CREATE TABLE IF NOT EXISTS `y_2001`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` VARCHAR(50),
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_2002`;
CREATE TABLE IF NOT EXISTS `y_2002`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` VARCHAR(50),
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` INT,
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_2003`;
CREATE TABLE IF NOT EXISTS `y_2003`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` VARCHAR(50),
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` VARCHAR(50),
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_2004`;
CREATE TABLE IF NOT EXISTS `y_2004`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` VARCHAR(50),
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` VARCHAR(50),
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_2005`;
CREATE TABLE IF NOT EXISTS `y_2005`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` VARCHAR(50),
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` VARCHAR(50),
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_2006`;
CREATE TABLE IF NOT EXISTS `y_2006`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` VARCHAR(50),
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` VARCHAR(50),
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_2007`;
CREATE TABLE IF NOT EXISTS `y_2007`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` VARCHAR(50),
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` VARCHAR(50),
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_2008`;
CREATE TABLE IF NOT EXISTS `y_2008`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` VARCHAR(50),
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` VARCHAR(50),
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `y_2015`;
CREATE TABLE IF NOT EXISTS `y_2015`(`Year` INT,
									`Month` INT,
									`Day_Of_Month` INT,
									`Day_Of_Week` INT,
									`Airline_Code` VARCHAR (10),
									`Flight_Num` INT,
									`Tail_Num` VARCHAR(50),
									`Origin_Airport` VARCHAR(50),
									`Dest_Airport` VARCHAR(50), 
									`Sched_Dep_Time` TIME,
									`Dep_Time` TIME,
									`Dep_Delay` INT,
									`Sched_Arr_Time` TIME,
									`Arr_Time` TIME,
									`Arr_Delay` INT,
									`Taxi_Out` INT,
                                    `Wheels_Off` TIME,
                                    `Wheels_On` TIME,
									`Taxi_In` INT,
									`Sched_Elapsed_Time` INT,
									`Elapsed_Time` INT, 
									`Air_Time` INT, 
									`Distance` INT,
									`Diverted` INT,
									`Cancelled` INT,
									`Cancellation_Reason` VARCHAR(50),
									`NASDelay` INT,
									`Security_Delay` INT,
									`Airline_Delay` INT, 
									`Late_Aircraft_Delay` INT,
									`Weather_Delay` INT,
                                    FOREIGN KEY (`Airline_Code`) REFERENCES `airline`(`Code_IATA`),
                                    FOREIGN KEY (`Origin_Airport`) REFERENCES `airport`(`Code_IATA`),
                                    FOREIGN KEY (`Dest_Airport`) REFERENCES `airport`(`Code_IATA`)
);

DROP TABLE IF EXISTS `description`;
CREATE TABLE IF NOT EXISTS `description`(`Variable` VARCHAR(30),
										 `Description` VARCHAR(100)
);


/*
ALTER TABLE `y_1987`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_1988`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_1989`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_1990`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_1991`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_1992`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_1993`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_1994`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_1995`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_1996`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_1997`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_1998`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_1999`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_2000`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_2001`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_2002`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_2003`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_2004`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_2005`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_2006`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_2007`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_2008`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE `y_2015`
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

*/























