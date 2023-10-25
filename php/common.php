<?php

function send_error() {
  http_response_code(500);
  echo "Internal Server Error";
  exit();
}

function read_from_config($config_array, $value_name) {
 if (array_key_exists($value_name, $config_array)) 
  {
   return $config_array[$value_name];
  } 
 else
   {
    send_error();
   }  
}

function return_json($data) { 
 header('Content-Type: application/json; charset=utf-8');
 http_response_code(200);
 echo json_encode($data);
}

function read_config() {
 return parse_ini_file("config.ini");
}

function connect_to_db() {
 $config_array = read_config();
 $db_user = read_from_config($config_array, "db_user");
 $db_password = read_from_config($config_array, "db_password");
 try{
    return mysqli_connect("localhost", $db_user, $db_password, "monitor"); 
 } catch(Exception $e) {
    echo $e;
    send_error();
  } 
}

function map_mysql_row_to_data($result) {
  return array(
                "timestamp" => intval($result["timestamp"]), 
                "temperature" => floatval($result["temperature"]), 
                "humidity" => floatval($result["humidity"])
                );
}

?>