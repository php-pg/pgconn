<?php /** @noinspection ForgottenDebugOutputInspection */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

//$dsn = 'postgres://password:user@127.0.0.1:5432/test_db';
$dsn = 'postgres://php_pg_md5:secret@127.0.0.1:5433/php_pg_test';
$config = \PhpPg\PgConn\Config\Internal\parseConfig($dsn);

$conn = (new \PhpPg\PgConn\PgConnector())->connect($config);

$sql = "SELECT unnest(ARRAY[1, 2, 3]::int[]) AS id, unnest(ARRAY['how', 'are', 'you']::text[]) AS txt";
$mrr = $conn->exec($sql);

$result = $mrr->readAll()[0];

$assocResults = [];
$fieldDescriptions = $result->getFieldDescriptions();

foreach ($result->getRows() as $rowIdx => $row) {
    foreach ($row as $colIdx => $colVal) {
        $assocResults[$rowIdx][$fieldDescriptions[$colIdx]->name] = $colVal;
    }
}


/**
 * Prints:
 * Array
 *  (
 *    [0] => Array
 *        (
 *            [id] => 1
 *            [txt] => how
 *        )
 *
 *    [1] => Array
 *        (
 *            [id] => 2
 *            [txt] => are
 *        )
 *
 *    [2] => Array
 *        (
 *            [id] => 3
 *            [txt] => you
 *        )
 *  )
 */
print_r($assocResults);
