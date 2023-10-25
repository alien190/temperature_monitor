<?php
require __DIR__.'/common.php';


$db_connection = connect_to_db();

try {
  $result = mysqli_query($db_connection, "SELECT * FROM measurings ORDER BY timestamp DESC LIMIT 1");
  $result_array = mysqli_fetch_array($result);
  $data = map_mysql_row_to_data($result_array);
  return_json($data);
} catch(Exception $e) {
  echo $e;
  send_error();
}
?>
