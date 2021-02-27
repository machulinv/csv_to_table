<?php

//Тестовое задание - 
//Нужно написать парсер csv файлов с данными (в качестве разделителя любой удобный символ):

//1. папка /import/ с вложенными подпапкам /YYYY/MM/DD в конечной папке несколько csv файлов
//2. поля в файлах такие "рег. номер, наименование, url, телефон, email"
//3. необходимо собрать все данные из файлов в mysql таблицу
//4. повторная обработка файла в будущем не допускается
//5. название конечных полей на усмотрение исполнителя

//все события нужно логировать в отдельную таблицу

///////////////////////////////////////////////////

//Путь к папке без косой черты в конце
//Папка должна быть на том же компьютере где SQL сервер
$path = "c:/test"; 

$host = "localhost"; 
$username = "root";
$password = "";

//Имя существующей базы данных
$database = "test";

//Имя таблицы для записи из csv файлов
$csvtable = "csvdata_test";

//Имя таблицы для логов
$logtable = "log_test";

 //Разделитель полей в csv файле
$field_sep = ",";

//Разделитель строк в csv файле
$line_sep = "\n";

//Кодировка csv файлов
$character_set = "cp1251";

//Кодировка имен файлов
$file_name_characterset = "cp1251";

///////////////////////////////////////////////////

$basedir = $path."/import/";
$basedir_len = strlen($basedir);
$date_len = 10; //Длина подстроки YYYY/MM/DD в пути к файлу

$files = glob( $basedir."[0-9][0-9][0-9][0-9]/[0-1][0-9]/[0-3][0-9]/*.csv" );

$con = mysqli_connect($host, $username, $password, $database);

//Проверка соединения
if (mysqli_connect_errno()) {
  echo "Невозможно подключиться к базе данных $database: " . mysqli_connect_error()."\n";
  exit(); }

$efield_sep = $con->real_escape_string($field_sep);
$eline_sep = $con->real_escape_string($line_sep);

//Создание таблицы для логов
if( !$con->query("CREATE TABLE IF NOT EXISTS $logtable (time DATETIME, message TEXT) CHARACTER SET 'utf8'") ){
	echo "Не удалось создать таблицу $logtable: (" . $con->errno . ") " . $con->error."\n";
	exit(); }

//Записывает логи и в консоль и в таблицу
function log1($con, $logtable, $message)
{
	$timestr = date("Y-m-d H:i:s");

	echo $timestr." ".$message."\n";

	$message = $con->real_escape_string($message);
	$sql = "INSERT INTO $logtable (time, message) VALUES ('$timestr', '$message')";

	if( !$con->query($sql) ) {
		echo "$timestr Невозможно добавить запись в таблицу $logtable: ($con->errno) $con->error"."\n"; 
		exit(); }
}

function show_warnings( $con, $logtable, $file )
{
	$j = mysqli_warning_count($con);

	if ($j > 0) {
		$e = mysqli_get_warnings($con);

		for ($i = 0; $i < $j; $i++) {
			log1($con, $logtable, "Предупреждение при  записи файла $file в базу данных : ($e->errno) $e->message");
			$e->next();
		}
	}
}


//Установка кодировки для имен файлов
if( !$con->query("SET SESSION character_set_filesystem = '$file_name_characterset'") ){
	log1($con, $logtable, "Не удалось установить кодировку имен файлов: ($con->errno) $con->error");  
	exit(); }

//Создание таблицы для данных из csv файлов
if( !$con->query("CREATE TABLE IF NOT EXISTS $csvtable (date VARCHAR(10), file_name VARCHAR(255), reg_number VARCHAR(50), name VARCHAR(80), url TEXT, phone VARCHAR(30), email TEXT) CHARACTER SET $character_set") ){
	log1($con, $logtable, "Не удалось создать таблицу $csvtable: ($con->errno) $con->error");  
	exit(); }

//Чтение данных из всех csv файлов
foreach( $files as $file ) {

	$date = substr($file, $basedir_len, $date_len);
	$file_name = substr($file, $basedir_len + $date_len + 1);
	$file_name = $con->real_escape_string($file_name);
	$date = $con->real_escape_string($date);

	//Проверка записан ли файл в базу данных. Проверка выполняется по именам файла и папок YYYY/MM/DD
	$sql = "SELECT * FROM $csvtable WHERE date = '$date' AND file_name = '$file_name' LIMIT 1";

	if( !$result = $con->query($sql) ) {
		log1($con, $logtable, "Не удалось сделать запрос к таблице $csvtable: ($con->errno) $con->error");
		exit(); }

	if( !$result->num_rows ) {
		
		//Чтение данных из csv файла
		$sql = "LOAD DATA INFILE '$file' INTO TABLE $csvtable CHARACTER SET $character_set FIELDS TERMINATED BY '$efield_sep' LINES TERMINATED BY '$eline_sep' (reg_number, name, url, phone, email) SET date = '$date', file_name = '$file_name'";

		if( !$con->query($sql) ) {
			log1($con, $logtable, "Не удалось записать файл $file в таблицу $csvtable: ($con->errno) $con->error");}

		show_warnings($con, $logtable, $file);
	} else
	{
		log1($con, $logtable, "Файл $file уже был записан в таблицу $csvtable");
	}

	$result->close();

}


?>
