<?php
class MysqlTransporter
{
    /**
     * requirements 
     * 
     * update and insert fields need to be in every table a
     * and need to be of datatype timestamp 
     * the insert value van never be empty (is mandatory)
     * 
     * there must be a table names transporterruns
     * sql : CREATE TABLE transporterruns ( rundate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP , PRIMARY KEY (rundate)) ENGINE = InnoDB; 
     * insert the first row : INSERT INTO `transporterruns` (`rundate`) VALUES ('2021-01-01 00:00:00'); 
     * 
     */
    protected $_db;
    protected $_log;
    protected $_insertField = "created_at";
    protected $_updateField = "updated_at";
    protected $_dateLastRun;
    protected $_filename;

    protected $_lines = [];

    function __construct(MysqlDatabase $db, Log $log)
    {
        $this->_db = $db;
        $this->_log = $log;
    }

    function start(string $database) : void
    {
        /** prepare a folder and a filename */
        $this->_createFolder($database);

        $sql = "SELECT MAX(rundate) AS rundate FROM transporterruns";
        $rows = $this->_db->select($sql);
        $this->_dateLastRun = $rows[0]['rundate'];
    }

    function _isProcessible(string $database, string $table) : bool
    {
        /** make sure the fields mentioned exist in the table */
        $sql = "SELECT DISTINCT(table_name) 
                FROM information_schema.columns 
                WHERE TABLE_SCHEMA = ?
                AND table_name = ?
                AND table_name NOT IN 
                    (SELECT table_name 
                    FROM information_schema.columns 
                    WHERE TABLE_SCHEMA = ? and column_name = ? or column_name = ? )";

        $rows = $this->_db->select($sql, "sssss", [$database, $table, $database, $this->_insertField, $this->_insertField]);
        if (count($rows) > 0) 
        {
            return false;
        } else {
            return true;
        }
    }

    function processInserts(string $database, string $table) : bool
    {
        if ($this->_isProcessible($database, $table))
        {
            /** process the inserts */
            $sql = "SELECT * FROM {$database}.{$table} WHERE {$this->_insertField} >= ?";
            $rows = $this->_db->select($sql, "s", [$this->_dateLastRun]);

            $this->queries = [];
            foreach ($rows as $row)
            {
                $queries[] = $this->createInsertStatement($database, $table, $row);
            }

            if (!empty($queries)) 
            {
                $this->_lines[] = "-- Inserts for the {$table} table --";
                foreach ($queries as $sql)
                {
                    $this->_lines[] = $sql;
                }
            }
        } else {
            return false;
        }
        return true;
    }
            
    function processUpdates(string $database, string $table) : bool
    {
        if ($this->_isProcessible($database, $table))
        {
            /** process the updates */
            $sql = "SELECT * FROM {$database}.{$table} WHERE {$this->_updateField} >= ?";
            $rows = $this->_db->select($sql, "s", [$this->_dateLastRun]);

            $queries = [];
            foreach ($rows as $row)
            {
                $queries[] = $this->createUpdateStatement($database, $table, $row);
            }

            if (!empty($queries)) 
            {
                $this->_lines[] = "-- Updates for the {$table} table --";
                foreach ($queries as $sql)
                {
                    $this->_lines[] = $sql;
                }
            }

        } else {
            return false;
        }
        return true;
    }    

    function finish(string $database) : void
    {
        /** write to a file */
        if (!empty($this->_lines))
        {
            echo "writing to: $this->_filename";
            $file = fopen($this->_filename, "w");
            fwrite($file, implode("\n", $this->_lines));
            fclose($file);

            $total = count($this->_lines);
            echo "<p>{$total} modified records for database {$database}</p>";
   
        } else {
            echo "<p>No updates for database {$database}</p>";
        }

        /** update the transporter runs */
        $timestamp = date("Y-m-d H:i:s");
        $sql = "INSERT INTO {$database}.transporterruns (`rundate`) VALUES (?)";
        $result = $this->_db->insert($sql, "s", [$timestamp]);

    }

    /** todo move this to the mysql administrator class */
    function getPrimaryKey(string $database, string $table) : string
    {
        $sql = "SELECT k.column_name FROM information_schema.table_constraints t JOIN information_schema.key_column_usage k USING(constraint_name,table_schema,table_name) WHERE t.constraint_type = ? AND t.table_schema = ? AND t.table_name = ? ";

        /** todo support a primary key collection */
        $pk = $this->_db->select($sql, "sss", ["PRIMARY KEY", $database, $table]);
        return $pk[0]['column_name'];
    }

    /** todo move this to the mysql administrator class */
    function getTableColumns(string $database, string $table) : array
    {
        $sql = "SELECT column_name, data_type
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
            AND TABLE_NAME= ?";

        $rows = $this->_db->select($sql, "ss", [$database, $table]);
        foreach ($rows as $row)
        {
            $key = $row['column_name'];
            $cols[$key] = $row;
        }
        return $cols;
    }

    function createInsertStatement(string $database, string $table, array $row) : string
    {
        /** get the columns */
        $columns = $this->getTableColumns($database, $table);

        $values = [];
        foreach ($row as $key => $item)
        {
            $datatype = $columns[$key]['data_type'];
            $fields[] = $key;
            $values[] = (empty($item) && !is_numeric($item) ? "NULL" : $this->_format($item, $datatype));
        }

        $fields = implode(", ", $fields);
        $values = implode(", ", $values);

        $sql = "INSERT INTO {$table} ({$fields})\n VALUES ({$values});";

        return $sql;
    }

    function createUpdateStatement(string $database, string $table, array $row) : string
    {
        /** get the columns */
        $columns = $this->getTableColumns($database, $table);
        $pk = $this->getPrimaryKey($database, $table);

        $values = "";
        $pk_value = 1;
        foreach ($row as $key => $item)
        {
            if ($key === $pk)
            {
                $pk_value = $item;
            } else {
                $datatype = $columns[$key]['data_type'];
                $values = (empty($values) ? $values : $values . ", ");
                $values .= $key . "=" . (empty($item) && !is_numeric($item) ? "NULL" : $this->_format($item, $datatype));
            }
        }

        $sql = "UPDATE {$table} SET {$values} WHERE {$pk} = {$pk_value};";
        return $sql;
    }

    private function _format(string $item, string $datatype) : string 
    {
        switch ($datatype)
        {
            /** date and time types */
            case "date":
            case "timestamp":
                $item = "\"" . $this->_db->realEscapeString($item) . "\"";
                break;

            /** numeric types */
            case "tinyint":
            case "int":
            case "year":                
                /** do nothing */
                break;

            /** text types */
            case "text":
            case "longtext":
            case "char":
            case "varchar":
                $item = "\"" . $this->_db->realEscapeString($item) . "\"";
                break;
            default:
                throw new exception("Undefined datatype {$datatype} provided");
        }
        return $item;
    }

    private function _createFolder(string $database)
    {
        $step = 10;
        $filename = date('Ymd') . "_" . $step . "_update_{$database}.sql";
        while (file_exists($filename))
        {
            $step += 10;
            $filename = date('Ymd') . "_" . $step . "_update_{$database}.sql";
        }
        $folder = "../output/{$database}";

        if (!file_exists($folder))
        {
            if(!mkdir($folder, 0777, true)) {
                die('Failed to create {$folder}...');
            }
        } else {
            echo "Folder {$folder} exists.";
        }

        $this->_filename = $folder . "/" . $filename;        
    }

    function getProcessOrder(string $database) : array
    {
        /** get all the tables from the database */
        $sql = "SELECT table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE' 
                AND table_schema = ?";

        $rows = $this->_db->select($sql, "s", [$database]);

        /** create an array */
        $tables = [];
        foreach ($rows as $row)
        {
            $table = $row['table_name'];
            $tables[$table] = [];
        }
        
        /** get the parents */
        $keys = array_keys($tables);
        foreach ($keys as $key)
        {
            /** get its parents */
            $sql = "SELECT referenced_table_name
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                    WHERE constraint_schema = ?
                    AND table_name = ?
                    AND constraint_name != 'PRIMARY'
                    ORDER BY table_name";
            $rows = $this->_db->select($sql, "ss", [$database, $key]);

            $parents = [];
            foreach ($rows as $row)
            {
                if (!empty($row['referenced_table_name']))
                {
                    $parents[] = $row['referenced_table_name'];
                }
            }
            $tables[$key] = $parents;
        }
        $processorder = [];

        while (!empty($tables))
        {
            $keys = array_keys($tables);

            foreach ($keys as $key)
            {
                /** if the parents array is empty */
                if (empty($tables[$key]))
                {
                    $this->_log->write("key : {$key} is leeg");
                    $processorder[] = $key;
                    unset($tables[$key]);
                } elseif (empty(array_diff($tables[$key], $processorder))) {
                    $this->_log->write("key : {$key} diff is gelijk");

                    $processorder[] = $key;
                    unset($tables[$key]);
                }
            }
        }

        return $processorder;
    }
     
}
?>