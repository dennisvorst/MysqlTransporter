<?php
require_once "class/MysqlDatabase.php";

$database = (empty($_GET['database']) ? "" : $_GET['database']);
$table = (empty($_GET['table']) ? "" : $_GET['table']);

try{
    $db = new MysqlDatabase();

    echo "<h1>Database</h1>\n";
    if (empty($database))
    {
        $databases = $db->getDatabases();

        foreach ($databases as $database)
        {
            $database = $database['TABLE_SCHEMA'];
            echo "<a href='index.php?database={$database}'>{$database}</a><br>\n";
        }
    } else {
        echo "<a href='index.php?database={$database}'>{$database}</a><br>\n";

        echo "<h1>Tables</h1>";

        if (empty($table))
        {
            $tables = $db->getTables($database);
            foreach ($tables as $table)
            {
                $table = $table['TABLE_NAME'];
                echo "<a href='index.php?database={$database}&table={$table}'>{$table}</a><br>\n";
            }

            echo "<br>";
            echo "<a href='index.php?database={$database}&process=all'>process all tables</a><br>\n";
    
        } else {
            echo "<a href='index.php?database={$database}&table={$table}'>{$table}</a><br>\n";

            process($db, $database, $table);
        }
        echo "<br>";
        echo "<a href='index.php'>Restart</a><br>\n";
    }
} catch (Exception $e) {
    echo 'Caught exception: ',  $e->getMessage(), "\n";
}

function process(MysqlDatabase $db, string $database, string $table)
{
    $folder = init($database, $table);

    exportTable($db, $database, $table, $folder);

}

function exportTable(MysqlDatabase $db, string $database, string $table, string $outputFolder)
{
    /** init */
    $dateCreatedField = "created_at";
    $dateUpdatedField = "updated_at";
    $hasUpdatedField = False;
    $hasCreatedField = False;
    $fields = [];

    /** get the latest created file */
    $time = getLastRunDate($outputFolder);

    $sql = "SELECT * FROM {$database}.{$table}";
    if ($time > 0)
    {
        /** it has been run before */
        $dateLastRun = new DateTime();
        $dateLastRun->setTimestamp($time);

        /** format shuld be 2019-03-24 09:15:09 */
        $dateValue = $dateLastRun->format('Y-m-d H:i:s');
        $sql .= " WHERE {$dateCreatedField} >= '{$dateValue}'
            OR {$dateUpdatedField} >= '{$dateValue}'
        ";    
    }

    /** get the rows */
    $rows = $db->queryDb($sql);

    if (empty($rows))
    {
        echo "<p>No records to process</p>";
        return;
    }

    /** create the file name  */
    $time = new DateTime();
    $time = $time->getTimestamp();
    $file = $outputFolder . "{$time}_{$database}_{$table}.sql";

    /** open the file for writing*/
    $file = fopen($file, "w");

    /** get the columns  */
    $columns = $db->getColumns($database, $table);

    foreach ($columns as $column)
    {
        $column_name = $column['column_name'];
        /** is the created field present? */
        if ($column_name == $dateCreatedField)
        {
            $hasCreatedField = true;
        }
        /** is the updated field present */
        if ($column_name == $dateUpdatedField)
        {
            $hasUpdatedField = true;
        }
        $fields[$column_name] = $column;
    }

    if (!$hasUpdatedField || !$hasCreatedField)
    {
        throw new exception("No way to determine when the row was created or updated.");
    } 

    /** create the fieldlist for the insertstatement */
    $keys = array_keys($fields);
    $fieldList = implode(", ", $keys);

    /** process the rows */
    foreach ($rows as $row)
    {
        $insert = true;
        $values = [];

        foreach ($keys as $key)
        {
            $values[] = createFieldContect($row[$key], $fields[$key]);

            /** if the fieldname is the datecreatedfieldname 
             * and its value is before the last run date 
             * we only need to update it
             */
            if ($key == $dateCreatedField && !empty($dateValue) && $row[$key] < $dateValue)
            {
                $insert = false;
            } 
        }

        if ($insert)
        {
            $values = implode(", ", $values);

//            $line = "INSERT INTO {$database}.{$table} ({$fieldList}) VALUES ({$values});\n";
            $line = "INSERT INTO {$table} ({$fieldList}) VALUES ({$values});\n";
    
        } else {
            $line = "UPDATE {$table} SET ";
        }
        /** write to the file */
        fwrite($file, $line);
    }   

    /** close the file */
    fclose($file);
}

function createFieldContect($value, array $properties)
{
    print_r($properties);
    print_r("<br>");

    if (empty($value) && $properties['is_nullable'] == "YES")
    {
        return "NULL";
    } 

    $data_type = $properties['data_type'];
    switch ($data_type)
    {
        case "int":
            return $value;
            break;
        case "text":
        case "char":
        case "varchar":
            return "\"" . $value . "\"";
            break;

        case "datetime":
            return "'{$value}'";
            break;
                         
        default: 
            throw new exception("Datatype {$data_type} currently not supported.");
    } 
    return $value;

}


/** create an output folder that has the name of the database 
 * and witin that folder create the folder with the name of the table 
 * 
 * Furthermore we assume the output folder and its content should be created in the upper folder
 * */
function init(string $database, string $table) : string
{

    $folder = "../output/" . $database . "/" . $table . "/";
    if (!file_exists($folder))
    {
        echo "<p>Folder is being created</p>\n";
        mkdir($folder, 0777, true);
    } else {
        echo "<p>Folder already exists</p>\n";
    }
    return $folder;
}

/** what is the last time in the outputfolder */
function getLastRunDate(string $folder) : int
{
    $files = scandir($folder);
    if (count($files) == 2)
    {
        return -1;
    } else {
        $file = $files[count($files) -1];
        $file = substr($file, 0, 10);
        return $file;    
    }
}
?>

