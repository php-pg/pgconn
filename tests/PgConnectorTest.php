<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Tests;

use Amp\Socket\EncryptableSocket;
use Amp\TimeoutCancellation;
use PhpPg\PgConn\Config\AfterConnectFuncInterface;
use PhpPg\PgConn\Config\HostConfig;
use PhpPg\PgConn\Config\ValidateConnect\ValidateConnectReadWrite;
use PhpPg\PgConn\Config\ValidateConnectFuncInterface;
use PhpPg\PgConn\Exception\ConnectException;
use PhpPg\PgConn\Exception\PgErrorException;
use PhpPg\PgConn\PgConn;
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

        $this->expectNotToPerformAssertions();

        $config = parseConfig($connString);
        $connector = new PgConnector();
        $connection = $connector->connect($config, new TimeoutCancellation(2));
        $connection->close();
    }

    /**
     * @testWith ["PG_TEST_TLS_CONN_STRING"]
     * @return void
     */
    public function testConnectTLS(string $env): void
    {
        $connString = getenv($env);
        if (false === $connString || '' === $connString) {
            $this->markTestSkipped("Missing env {$env}");
        }

        $config = parseConfig($connString);
        $connector = new PgConnector();

        $connection = $connector->connect($config, new TimeoutCancellation(2));
        $connection->close();

        $socket = $connection->getSocket();
        self::assertInstanceOf(EncryptableSocket::class, $socket);
        self::assertSame(EncryptableSocket::TLS_STATE_ENABLED, $socket->getTlsState());
    }

    public function testConnectTimeout(): void
    {
        $connString = getenv('PG_TEST_TCP_CONN_STRING');
        if (false === $connString || '' === $connString) {
            $this->markTestSkipped("Missing PG_TEST_TCP_CONN_STRING env var");
        }

        $this->markTestIncomplete('Test is not implemented');
//        $this->expectException(ConnectException::class);

//        $config = parseConfig($connString)->withConnectTimeout(0.01);
//        $res = (new PgConnector())->connect($config);
    }

    public function testConnectWithInvalidUser(): void
    {
        $connString = getenv('PG_TEST_TCP_CONN_STRING');
        if (false === $connString || '' === $connString) {
            $this->markTestSkipped("Missing PG_TEST_TCP_CONN_STRING env var");
        }

        $this->expectException(ConnectException::class);

        $config = parseConfig($connString)->withUser('pgxinvalidusertest');

        $connector = new PgConnector();

        try {
            $connector->connect($config, new TimeoutCancellation(2));
        } catch (ConnectException $e) {
            $prev = $e->getPrevious();

            self::assertInstanceOf(PgErrorException::class, $prev);
            self::assertContains($prev->pgError->sqlState, ['28P01', '28000']);

            throw $e;
        }
    }

    public function testConnectWithConnectionRefused(): void
    {
        $config = parseConfig('host=127.0.0.1 port=1 connect_timeout=0.5');
        $connector = new PgConnector();

        $this->expectException(ConnectException::class);

        $connector->connect($config, new TimeoutCancellation(2));
    }

    public function testConnectWithRuntimeParams(): void
    {
        $connString = getenv('PG_TEST_CONN_STRING');
        if (false === $connString || '' === $connString) {
            $this->markTestSkipped("Missing PG_TEST_CONN_STRING env var");
        }

        $config = parseConfig($connString)
            ->withRuntimeParam('application_name', 'pg_test')
            ->withRuntimeParam('search_path', 'myschema');

        $connector = new PgConnector();

        $conn = $connector->connect($config, new TimeoutCancellation(2));
        $results = $conn->exec('show application_name; show search_path;')->readAll();

        self::assertSame('pg_test', $results[0]->getRows()[0][0]);
        self::assertSame('myschema', $results[1]->getRows()[0][0]);
    }

    public function testConnectWithFallback(): void
    {
        $connString = getenv('PG_TEST_CONN_STRING');
        if (false === $connString || '' === $connString) {
            $this->markTestSkipped("Missing PG_TEST_CONN_STRING env var");
        }

        $this->expectNotToPerformAssertions();

        $config = parseConfig($connString);
        $config = $config
            ->withConnectTimeout(1)
            ->setHosts(
                \array_merge(
                    [
                        new HostConfig(host: '127.0.0.1', port: 1, password: '')
                    ],
                    $config->getHosts()
                )
            );

        $connector = new PgConnector();
        $connector->connect($config, new TimeoutCancellation(3));
    }

    public function testConnectWithValidateConnect(): void
    {
        $connString = getenv('PG_TEST_CONN_STRING');
        if (false === $connString || '' === $connString) {
            $this->markTestSkipped("Missing PG_TEST_CONN_STRING env var");
        }

        $counter = 0;

        $config = parseConfig($connString)
            ->withValidateConnectFunc(new class($counter) implements ValidateConnectFuncInterface {
                public function __construct(private int &$counter)
                {
                }

                public function __invoke(PgConn $conn): void
                {
                    $this->counter++;
                    if ($this->counter < 2) {
                        throw new \RuntimeException('bad connection');
                    }
                }
            });

        $config = $config->setHosts([$config->getHosts()[0], $config->getHosts()[0]]);

        $connector = new PgConnector();
        $connector->connect($config, new TimeoutCancellation(2));

        self::assertSame(2, $counter);
    }

    public function testConnectWithValidateConnectTargetSessionAttrsReadWrite(): void
    {
        $connString = getenv('PG_TEST_CONN_STRING');
        if (false === $connString || '' === $connString) {
            $this->markTestSkipped("Missing PG_TEST_CONN_STRING env var");
        }

        $config = parseConfig($connString)
            ->withValidateConnectFunc(new ValidateConnectReadWrite())
            ->withRuntimeParam('default_transaction_read_only', 'on');

        $this->expectException(ConnectException::class);

        $connector = new PgConnector();
        $connector->connect($config, new TimeoutCancellation(2));
    }

    public function testConnectAfterConnect(): void
    {
        $connString = getenv('PG_TEST_CONN_STRING');
        if (false === $connString || '' === $connString) {
            $this->markTestSkipped("Missing PG_TEST_CONN_STRING env var");
        }

        $config = parseConfig($connString)
            ->withAfterConnectFunc(new class implements AfterConnectFuncInterface {
                public function __invoke(PgConn $conn): void
                {
                    $conn->exec('set search_path to foobar')->readAll();
                }
            });

        $connector = new PgConnector();
        $conn = $connector->connect($config, new TimeoutCancellation(2));

        $results = $conn->exec('show search_path')->readAll();
        self::assertSame('foobar', $results[0]->getRows()[0][0]);
    }
}
