<?php

namespace PhpPg\PgConn\Tests;

use Amp\CancelledException;
use Amp\TimeoutCancellation;
use PhpPg\PgConn\Exception\PgErrorException;
use PhpPg\PgConn\PgConn;
use PhpPg\PgConn\PgConnector;
use PHPUnit\Framework\TestCase;

use function Amp\delay;
use function PhpPg\PgConn\Config\Internal\parseConfig;

class PgConnTest extends TestCase
{
    protected PgConn $conn;

    protected function setUp(): void
    {
        parent::setUp();

        $connString = getenv('PG_TEST_CONN_STRING');
        if (false === $connString || '' === $connString) {
            $this->markTestSkipped("Missing PG_TEST_CONN_STRING env var");
        }

        $handler = new \Amp\Log\StreamHandler(\Amp\ByteStream\getStdout());
        $formatter = new \Amp\Log\ConsoleFormatter(
            format: "[%datetime%] %channel%.%level_name%: %message% %context%\r\n"
        );
        $handler->setFormatter($formatter);

        $logger = new \Monolog\Logger('pg');
        $logger->pushHandler($handler);

        $config = parseConfig($connString);
//        $config = $config->withLogger($logger);
        $this->conn = (new PgConnector())->connect($config);
    }

    private function ensureConnectionValid(): void
    {
        $res = $this->conn
            ->execParams(
                'select generate_series(1,$1)',
                ['3'],
                [],
                [],
                [],
                new TimeoutCancellation(1.5, 'Connection check exceeded time limit'),
            )
            ->getResult();

        self::assertCount(3, $res->rows);
        self::assertSame('1', $res->rows[0][0]);
        self::assertSame('2', $res->rows[1][0]);
        self::assertSame('3', $res->rows[2][0]);
    }

    public function testPrepareSyntaxError(): void
    {
        $this->expectException(PgErrorException::class);

        try {
            $this->conn->prepare('ps1', 'SYNTAX ERROR');
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testPrepareCancelled(): void
    {
        $cancellation = new TimeoutCancellation(0.0001);

        try {
            $this->conn->prepare(
                'ps1',
                'SELECT 1',
                [],
                $cancellation,
            );
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testExec(): void
    {
        try {
            $results = $this->conn->exec("select 'Hello, world'")->readAll();

            self::assertCount(1, $results);
            self::assertSame('SELECT 1', (string)$results[0]->commandTag);
            self::assertSame('Hello, world', $results[0]->rows[0][0]);
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testExecEmpty(): void
    {
        try {
            $mrr = $this->conn->exec(";");
            $results = [];
            while ($mrr->nextResult()) {
                $rr = $mrr->getResultReader();
                while ($rr->nextRow()) {
                    $results[] = $rr->getResult();
                }

                $rr->getCommandTag();
            }

            self::assertCount(0, $results);
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testExecMultipleQueries(): void
    {
        try {
            $results = $this->conn->exec("select 'Hello, world'; select 1")->readAll();

            self::assertCount(2, $results);

            self::assertSame('SELECT 1', (string)$results[0]->commandTag);
            self::assertSame('Hello, world', $results[0]->rows[0][0]);

            self::assertSame('SELECT 1', (string)$results[1]->commandTag);
            self::assertSame('1', $results[1]->rows[0][0]);
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testExecMultipleQueriesEagerFieldDescriptions(): void
    {
        try {
            $mrr = $this->conn->exec("select 'Hello, world' as msg; select 1 as num");

            self::assertTrue($mrr->nextResult());
            self::assertCount(1, $mrr->getResultReader()->getFieldDescriptions());
            self::assertSame('msg', $mrr->getResultReader()->getFieldDescriptions()[0]->name);

            $mrr->getResultReader()->close();

            self::assertTrue($mrr->nextResult());
            self::assertCount(1, $mrr->getResultReader()->getFieldDescriptions());
            self::assertSame('num', $mrr->getResultReader()->getFieldDescriptions()[0]->name);

            $mrr->getResultReader()->close();

            self::assertFalse($mrr->nextResult());
            $mrr->close();
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testExecMultipleQueriesError(): void
    {
        try {
            $err = null;
            $mrr = $this->conn->exec('select 1; select 1/0; select 1');

            try {
                $results = $mrr->readAll();
            } catch (PgErrorException $e) {
                $err = $e;
                $results = $mrr->getPartialResults();
            }

            self::assertInstanceOf(PgErrorException::class, $err);
            self::assertSame('22012', $err->pgError->sqlState);

            self::assertCount(1, $results);
            self::assertCount(1, $results[0]->rows);
            self::assertSame('1', $results[0]->rows[0][0]);
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testExecDeferredError(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support deferred constraint (https://github.com/cockroachdb/cockroach/issues/31632)');
        }

        $sql = <<<SQL
create temporary table t (
    id text primary key,
    n int not null,
    unique (n) deferrable initially deferred
);

insert into t (id, n) values ('a', 1), ('b', 2), ('c', 3);
SQL;
        $this->conn->exec($sql)->readAll();

        try {
            $err = null;

            try {
                $this->conn->exec("update t set n=n+1 where id='b' returning *")->readAll();
            } catch (PgErrorException $e) {
                $err = $e;
            }

            self::assertInstanceOf(PgErrorException::class, $err);
            self::assertSame('23505', $err->pgError->sqlState);
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testExecCancelled(): void
    {
        $this->expectException(PgErrorException::class);

        try {
            $this->conn->exec(
                'SELECT pg_sleep(1);',
                new TimeoutCancellation(0.1),
            )->readAll();
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testExecPreCancelled(): void
    {
        $this->expectException(CancelledException::class);

        $cancellation = new TimeoutCancellation(0.01);
        delay(0.1);

        try {
            $this->conn->exec('SELECT 1', $cancellation)->readAll();
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testExecParams(): void
    {
        try {
            $result = $this->conn->execParams('select $1::text as msg', ['Hello, world']);
            self::assertCount(1, $result->getFieldDescriptions());
            self::assertSame('msg', $result->getFieldDescriptions()[0]->name);

            $rowCount = 0;
            while ($result->nextRow()) {
                $rowCount++;

                self::assertSame('Hello, world', $result->getRowValues()[0]);
            }

            self::assertSame(1, $rowCount);
            self::assertSame('SELECT 1', (string)$result->getCommandTag());
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testExecParamsDeferredError(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support deferred constraint (https://github.com/cockroachdb/cockroach/issues/31632)');
        }

        $sql = <<<SQL
create temporary table t (
    id text primary key,
    n int not null,
    unique (n) deferrable initially deferred
);

insert into t (id, n) values ('a', 1), ('b', 2), ('c', 3);
SQL;
        $this->conn->exec($sql)->readAll();

        try {
            $err = null;

            try {
                $this->conn->execParams("update t set n=n+1 where id='b' returning *")->getResult();
            } catch (PgErrorException $e) {
                $err = $e;
            }

            self::assertInstanceOf(PgErrorException::class, $err);
            self::assertSame('23505', $err->pgError->sqlState);
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testExecParamsMaxNumberOfParams(): void
    {
        // Increase memory limit to store many parameters
        $oldLimit = \ini_set('memory_limit', '256M');

        $args = [];
        $params = [];
        for ($i = 0; $i < 65535; $i++) {
            $params[] = \sprintf('($%d::text)', $i + 1);
            $args[] = (string)$i;
        }

        $sql = 'values ' . \implode(', ', $params);

        try {
            $result = $this->conn->execParams($sql, $args)->getResult();
            self::assertCount(\count($params), $result->rows);
        } finally {
            \ini_set('memory_limit', $oldLimit);
            $this->ensureConnectionValid();
        }
    }

    public function testExecParamsTooManyParams(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Extended protocol limited to 65535 parameters');

        // Increase memory limit to store many parameters
        $oldLimit = \ini_set('memory_limit', '256M');

        $args = [];
        $params = [];
        for ($i = 0; $i < 65536; $i++) {
            $params[] = \sprintf('($%d::text)', $i + 1);
            $args[] = (string)$i;
        }

        $sql = 'values ' . \implode(', ', $params);

        try {
            $result = $this->conn->execParams($sql, $args)->getResult();
        } finally {
            \ini_set('memory_limit', $oldLimit);
            $this->ensureConnectionValid();
        }
    }

    public function testExecParamsCancelled(): void
    {
        $this->expectException(PgErrorException::class);

        try {
            $result = $this->conn->execParams(
                sql: 'SELECT current_database(), pg_sleep(1);',
                cancellation: new TimeoutCancellation(0.1),
            )->getResult();
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testExecParamsPreCancelled(): void
    {
        $this->expectException(CancelledException::class);

        $cancellation = new TimeoutCancellation(0.01);
        delay(0.1);

        try {
            $this->conn->execParams(sql: 'SELECT 1', cancellation: $cancellation)->getResult();
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testExecParamsEmptySQL(): void
    {
        try {
            $res = $this->conn->execParams(sql: '')->getResult();

            self::assertCount(0, $res->fieldDescriptions);
            self::assertCount(0, $res->rows);
            self::assertSame('', (string)$res->commandTag);
        } finally {
            $this->ensureConnectionValid();
        }
    }


}