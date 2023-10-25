<?php
require __DIR__.'/common.php';

if(array_key_exists("period",$_GET)) {
  $periods_exp = "/m1|m5|m15|m30|h1|h4|d1/i";
  $period = $_GET["period"];
  if()
}


$db_connection = connect_to_db();

try {
  $result = mysqli_query($db_connection, "SELECT * FROM measurings ORDER BY timestamp ASC LIMIT 100");
  $result_array = array();
  while(1) {
   $result_row =  mysqli_fetch_array($result);
   if($result_row == null) break;

   $data = map_mysql_row_to_data($result_row);
   array_push($result_array, $data);
  }

  $result_to_send = array ("measurings" => $result_array);
  return_json($result_to_send);
} catch(Exception $e) {
  echo $e;
  send_error();
}
?>
