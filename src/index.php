<?php
require '../vendor/autoload.php';
require 'class/MysqlTransporter.php';

$database = (empty($_GET['database']) ? "" : $_GET['database']);
$table = (empty($_GET['table']) ? "" : $_GET['table']);
$title = "MysqlTransporter";

?>
<!DOCTYPE html>
<html dir="ltr" lang="en-US"><head>
	<!-- set the UTF-8 properties -->
	<!-- as defined in : https://www.toptal.com/php/a-utf-8-primer-for-php-and-mysql -->
    <meta charset="UTF-8">
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">

    <title><?php echo $title; ?></title>

    <!-- Latest compiled and minified CSS -->
	<link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">

	<!-- initiate font awesome -->
	<script src="https://kit.fontawesome.com/af1eec186a.js" crossorigin="anonymous"></script>

    <!--[if lt IE 9]><script src="https://html5shiv.googlecode.com/svn/trunk/html5.js"></script><![endif]-->
    <link rel="stylesheet" href="css/style.css" media="screen">
    <!--[if lte IE 7]><link rel="stylesheet" href="css/style.ie7.css" media="screen" /><![endif]-->
    <link rel="stylesheet" href="css/style.responsive.css" media="all">


	<style>
	.art-content .art-postcontent-0 .layout-item-0 { padding-right: 10px;padding-left: 10px;  }
    .ie7 .art-post .art-layout-cell {border:none !important; padding:0 !important; }
    .ie6 .art-post .art-layout-cell {border:none !important; padding:0 !important; }
    </style>
</head>
<body>
	<?php



try{
    $log = new Log("transporter.log");
    $mysqli = new Mysqli("localhost", "root", "", "museum");
    $db = new MysqlDatabase($mysqli, $log);

    echo "<h1>Database</h1>\n";
    if (empty($database))
    {
        /** get the databases */
        $sql = "SELECT SCHEMA_NAME FROM information_schema.schemata;";
        $databases = $db->select($sql);        

        foreach ($databases as $database)
        {
            $database = $database['SCHEMA_NAME'];
            echo "<a href='index.php?database={$database}'>{$database}</a><br>\n";
        }
    } else {
        echo "<a href='index.php?database={$database}'>{$database}</a><br>\n";
        $transporter = new MysqlTransporter($db, $log);
        $transporter->start($database);
        

        /** database is selected */
        echo "<h1>Tables</h1>";
        echo "<p>Processing</p>";

        $tables = $transporter->getProcessOrder($database);

        echo "<table><tr><th>Column</th><th>Inserts</th><th>Updates</th></tr>";

        foreach ($tables as $table)
        {
//            $table = $table['table_name'];
            echo "<tr><td>{$table}</td>";

            if ($transporter->processInserts($database, $table))
            {
                echo "<td>
                    <span style='color: MediumSeaGreen;'>
                        <i class='fas fa-check'></i>
                    </span>
                </td>";
            } else {
                echo "<td>
                    <span style='color: Tomato;'>
                        <i class='fas fa-times'></i>
                    </span>
                </td>";
                

            }
            if ($transporter->processUpdates($database, $table))
            {
                echo "<td>
                    <span style='color: MediumSeaGreen;'>
                    <i class='fas fa-check'></i>
                    </span>
                </td>";


            } else {
                echo "<td>
                    <span style='color: Tomato;'>
                        <i class='fas fa-times'></i></td>
                    </span>
                </td>";
            }
        }


        echo "</table>";

        echo "<br>";
        echo "<a href='index.php'>Restart</a><br>\n";

        $transporter->finish($database);

    }
} catch (Exception $e) {
    echo 'Caught exception: ',  $e->getMessage(), "\n";
}
?>

    <!-- unobtrusive javascript -->
    <!-- jQuery library -->
	<script src="https://code.jquery.com/jquery-3.2.1.min.js"
        integrity="sha256-hwg4gsxgFZhOsEEamdOYGBf13FyQuiTwlAQgxVSNgt4="
        crossorigin="anonymous"></script>
	<!-- if it is not loaded -->
	<script>window.jQuery || document.write('<script src="3rd/js/jquery-3.2.1.min.js"><\/script>');</script>

    <!-- Bootstrap JavaScript Library -->
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js"></script>
	<!-- if it is not loaded -->
	<script>window.jQuery || document.write('<script src="3rd/js/bootstrap.min.js"><\/script>');</script>
</body>
</html>