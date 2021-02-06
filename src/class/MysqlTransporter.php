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

    function __construct(MysqlDatabase $db, Log $log)
    {
        $this->_db = $db;
        $this->_log = $log;
    }

    function start(string $database) : void
    {
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
            $sql = "SELECT * FROM {$table} WHERE {$this->_insertField} >= ?";
            $rows = $this->_db->select($sql, "s", [$this->_dateLastRun]);

            foreach ($rows as $row)
            {
                print_r($row);
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
            /** process the inserts */
            $sql = "SELECT * FROM {$table} WHERE {$this->_insertField} < ? and {$this->_updateField} >= ?";
            $rows = $this->_db->select($sql, "ss", [$this->_dateLastRun, $this->_dateLastRun]);

            foreach ($rows as $row)
            {
                print_r($row);
            }
        } else {
            return false;
        }
        return true;
    }    

    function finish(string $database) : void
    {
        $timestamp = date("Y-m-d H:i:s");
        $sql = "INSERT INTO {$database}.transporterruns (`rundate`) VALUES (?)";
        $result = $this->_db->insert($sql, "s", [$timestamp]);
    }

}
?>
