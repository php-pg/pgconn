<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Tests;

use PhpPg\PgConn\PgConnector;
use PHPUnit\Framework\TestCase;

use function PhpPg\PgConn\Config\Internal\parseConfig;

class PgConnectorTest extends TestCase
{
    /**
     * @testWith ["PG_TEST_UNIX_SOCKET_CONN_STRING"]
     *  ["PG_TEST_TCP_CONN_STRING"]
     *  ["PG_TEST_PLAIN_PASSWORD_CONN_STRING"]
     *  ["PG_TEST_MD5_PASSWORD_CONN_STRING"]
     *  ["PG_TEST_SCRAM_PASSWORD_CONN_STRING"]
     * @return void
     */
    public function testConnect(string $env): void
    {
        $connString = getenv($env);
        if (false === $connString || '' === $connString) {
            $this->markTestSkipped("Missing env {$env}");
        }

        $config = parseConfig($connString);
        $connector = new PgConnector();
        $connection = $connector->connect($config);
        $connection->close();
    }
}
