<?php

namespace PhpPg\PgConn\Tests;

use Amp\ByteStream\ReadableIterableStream;
use Amp\ByteStream\WritableIterableStream;
use Amp\CancelledException;
use Amp\TimeoutCancellation;
use PhpPg\PgConn\Config\NoticeHandlerInterface;
use PhpPg\PgConn\Config\NotificationHandlerInterface;
use PhpPg\PgConn\Exception\LockException;
use PhpPg\PgConn\Exception\PgErrorException;
use PhpPg\PgConn\Notice;
use PhpPg\PgConn\Notification;
use PhpPg\PgConn\PgConn;
use PhpPg\PgConn\PgConnector;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;
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

//        $handler = new \Amp\Log\StreamHandler(\Amp\ByteStream\getStdout());
//        $formatter = new \Amp\Log\ConsoleFormatter(
//            format: "[%datetime%] %channel%.%level_name%: %message% %context%\r\n"
//        );
//        $handler->setFormatter($formatter);

//        $logger = new \Monolog\Logger('pg');
//        $logger->pushHandler($handler);

        $config = parseConfig($connString)->withConnectTimeout(2);
//        $config = $config->withLogger($logger);
        $this->conn = (new PgConnector())->connect($config);
    }

    protected function tearDown(): void
    {
        $this->conn->close();

        try {
            $suspension = EventLoop::getSuspension();
            $suspension->throw(new \RuntimeException('Catch you up!!!'));
        } catch (\Error) {
        }

        parent::tearDown();
    }

    private function ensureConnectionValid(?PgConn $conn = null): void
    {
        $res = ($conn ?? $this->conn)
            ->execParams(
                'select generate_series(1,$1)',
                ['3'],
                [],
                [],
                [],
                new TimeoutCancellation(1.5, 'Connection check exceeded time limit'),
            )
            ->getResult();

        self::assertCount(3, $res->getRows());
        self::assertSame('1', $res->getRows()[0][0]);
        self::assertSame('2', $res->getRows()[1][0]);
        self::assertSame('3', $res->getRows()[2][0]);
    }

    public function testPrepareSyntaxError(): void
    {
        $err = null;

        try {
            $this->conn->prepare('ps1', 'SYNTAX ERROR');
        } catch (PgErrorException $e) {
            $err = $e;
        }

        self::assertInstanceOf(PgErrorException::class, $err);

        $this->ensureConnectionValid();
    }

    public function testPrepareCancelled(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support query cancellation (https://github.com/cockroachdb/cockroach/issues/41335)');
        }

        // TODO: Test actually does nothing
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
        $results = $this->conn->exec("select 'Hello, world'")->readAll();

        self::assertCount(1, $results);
        self::assertSame('SELECT 1', (string)$results[0]->getCommandTag());
        self::assertSame('Hello, world', $results[0]->getRows()[0][0]);

        $this->ensureConnectionValid();
    }

    public function testExecEmpty(): void
    {
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

        $this->ensureConnectionValid();
    }

    public function testExecMultipleQueries(): void
    {
        $results = $this->conn->exec("select 'Hello, world'; select 1")->readAll();

        self::assertCount(2, $results);

        self::assertSame('SELECT 1', (string)$results[0]->getCommandTag());
        self::assertSame('Hello, world', $results[0]->getRows()[0][0]);

        self::assertSame('SELECT 1', (string)$results[1]->getCommandTag());
        self::assertSame('1', $results[1]->getRows()[0][0]);

        $this->ensureConnectionValid();
    }

    public function testExecMultipleQueriesEagerFieldDescriptions(): void
    {
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

        $this->ensureConnectionValid();
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
            self::assertCount(1, $results[0]->getRows());
            self::assertSame('1', $results[0]->getRows()[0][0]);
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
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support query cancellation (https://github.com/cockroachdb/cockroach/issues/41335)');
        }

        $this->expectException(PgErrorException::class);
        $this->expectExceptionMessage('ERROR: canceling statement due to user request (SQLSTATE 57014)');

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

        $this->ensureConnectionValid();
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

        $result = $this->conn->execParams($sql, $args)->getResult();
        self::assertCount(\count($params), $result->getRows());

        \ini_set('memory_limit', $oldLimit);
        $this->ensureConnectionValid();
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
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support query cancellation (https://github.com/cockroachdb/cockroach/issues/41335)');
        }

        $this->expectException(PgErrorException::class);
        $this->expectExceptionMessage('ERROR: canceling statement due to user request (SQLSTATE 57014)');

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
        $res = $this->conn->execParams(sql: '')->getResult();

        self::assertCount(0, $res->getFieldDescriptions());
        self::assertCount(0, $res->getRows());
        self::assertSame('', (string)$res->getCommandTag());

        $this->ensureConnectionValid();
    }

    public function testExecPrepared(): void
    {
        $stmt = $this->conn->prepare('ps1', 'select $1::text as msg');

        self::assertCount(1, $stmt->paramOIDs);
        self::assertCount(1, $stmt->fields);

        $rr = $this->conn->execPrepared('ps1', ['Hello World']);
        self::assertCount(1, $rr->getFieldDescriptions());

        $result = $rr->getResult();
        self::assertCount(1, $result->getRows());
        self::assertSame('SELECT 1', (string)$result->getCommandTag());

        $this->ensureConnectionValid();
    }

    public function testExecPreparedMaxNumberOfParams(): void
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

        $stmt = $this->conn->prepare('ps1', $sql);
        self::assertCount(\count($params), $stmt->paramOIDs);
        self::assertCount(1, $stmt->fields);

        $rr = $this->conn->execPrepared($stmt->name, $args);
        $result = $rr->getResult();

        self::assertCount(\count($params), $result->getRows());
        self::assertSame($args, \array_column($result->getRows(), 0));

        \ini_set('memory_limit', $oldLimit);
        $this->ensureConnectionValid();
    }

    public function testExecPreparedTooManyParams(): void
    {
        // Increase memory limit to store many parameters
        $oldLimit = \ini_set('memory_limit', '256M');

        $args = [];
        $params = [];
        for ($i = 0; $i < 65536; $i++) {
            $params[] = \sprintf('($%d::text)', $i + 1);
            $args[] = (string)$i;
        }

        $sql = 'values ' . \implode(', ', $params);


        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            // CockroachDB rejects preparing a statement with more than 65535 parameters.
            $this->expectException(PgErrorException::class);
            $this->expectExceptionMessage('ERROR: more than 65535 arguments to prepared statement: 65536 (SQLSTATE 08P01)');

            try {
                $this->conn->prepare('ps1', $sql, []);
            } finally {
                \ini_set('memory_limit', $oldLimit);
                $this->ensureConnectionValid();
            }
        } else {
            $stmt = $this->conn->prepare('ps1', $sql, []);
            // PostgreSQL accepts preparing a statement with more than 65535 parameters
            // and only fails when executing it through the extended protocol.
            self::assertCount(1, $stmt->fields);
            self::assertCount(\count($params), $stmt->paramOIDs);

            $this->expectException(\InvalidArgumentException::class);
            $this->expectExceptionMessage('Extended protocol limited to 65535 parameters');

            try {
                $this->conn->execPrepared($stmt->name, $args);
            } finally {
                \ini_set('memory_limit', $oldLimit);
                $this->ensureConnectionValid();
            }
        }
    }

    public function testExecPreparedCancelled(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support query cancellation (https://github.com/cockroachdb/cockroach/issues/41335)');
        }

        $this->expectException(PgErrorException::class);
        $this->expectExceptionMessage('ERROR: canceling statement due to user request (SQLSTATE 57014)');

        try {
            $stmt = $this->conn->prepare('ps1', 'SELECT current_database(), pg_sleep(1)');
            $result = $this->conn->execPrepared(
                stmtName: 'ps1',
                cancellation: new TimeoutCancellation(0.1),
            )->getResult();
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testExecPreparedPreCancelled(): void
    {
        $this->expectException(CancelledException::class);

        $cancellation = new TimeoutCancellation(0.01);
        delay(0.1);

        try {
            $stmt = $this->conn->prepare('ps1', 'SELECT current_database(), pg_sleep(1)');
            $result = $this->conn->execPrepared(
                stmtName: 'ps1',
                cancellation: $cancellation,
            )->getResult();
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testExecPreparedEmptySql(): void
    {
        $stmt = $this->conn->prepare('ps1', '');
        $result = $this->conn->execPrepared(stmtName: 'ps1')->getResult();

        self::assertSame('', (string)$result->getCommandTag());
        self::assertCount(0, $result->getRows());
        self::assertCount(0, $result->getFieldDescriptions());

        $this->ensureConnectionValid();
    }

    public function testConnLock(): void
    {
        $mrr = $this->conn->exec("SELECT 'Hello, world'");

        $err = null;
        try {
            $this->conn->exec("SELECT 'Hello, world'");
        } catch (LockException $e) {
            $err = $e;
        }

        self::assertInstanceOf(LockException::class, $err);
        self::assertSame('Lock error: Connection BUSY', $err->getMessage());

        $results = $mrr->readAll();
        self::assertCount(1, $results);
        self::assertSame('SELECT 1', (string)$results[0]->getCommandTag());
        self::assertCount(1, $results[0]->getRows());
        self::assertSame('Hello, world', $results[0]->getRows()[0][0]);

        $this->ensureConnectionValid();
    }

    public function testOnNotice(): void
    {
        $notice = null;
        $conf = $this->conn->getConfig()->withOnNotice(new class($notice) implements NoticeHandlerInterface {
            public function __construct(private &$notice)
            {
            }

            public function __invoke(Notice $notice): void
            {
                $this->notice = $notice;
            }
        });
        $conn = (new PgConnector())->connect($conf);

        if ($conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support PL/PGSQL (https://github.com/cockroachdb/cockroach/issues/17511)');
        }

        $mrr = $conn->exec(<<<SQL
do $$
begin
  raise notice 'Hello, world';
end$$;
SQL);

        $mrr->close();

        $this->ensureConnectionValid($conn);
        $conn->close();

        self::assertInstanceOf(Notice::class, $notice);
        self::assertSame('Hello, world', $notice->message);
    }

    public function testOnNotification(): void
    {
        $notification = null;
        $conf = $this->conn->getConfig()->withOnNotification(new class($notification) implements NotificationHandlerInterface {
            public function __construct(private &$notification)
            {
            }

            public function __invoke(Notification $notification): void
            {
                $this->notification = $notification;
            }
        });
        $conn = (new PgConnector())->connect($conf);

        if ($conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support LISTEN / NOTIFY (https://github.com/cockroachdb/cockroach/issues/41522)');
        }

        $conn->exec('LISTEN foo')->readAll();

        // Send notification
        $notifier = (new PgConnector())->connect($conf);
        $notifier->exec("NOTIFY foo, 'bar'")->readAll();
        $notifier->close();

        $conn->exec('SELECT 1')->readAll();
        $conn->close();

        self::assertInstanceOf(Notification::class, $notification);
        self::assertSame('bar', $notification->payload);

        $this->ensureConnectionValid();
    }

    public function testWaitForNotification(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support LISTEN / NOTIFY (https://github.com/cockroachdb/cockroach/issues/41522)');
        }

        $this->conn->exec('LISTEN foo')->readAll();

        // Send notification
        $notifier = (new PgConnector())->connect($this->conn->getConfig());
        $notifier->exec("NOTIFY foo, 'bar'")->readAll();
        $notifier->close();

        $notification = $this->conn->waitForNotification(new TimeoutCancellation(1));
        self::assertSame('bar', $notification->payload);

        $this->ensureConnectionValid();
    }

    public function testWaitForNotificationCancelled(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support query cancellation (https://github.com/cockroachdb/cockroach/issues/41335)');
        }

        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support LISTEN / NOTIFY (https://github.com/cockroachdb/cockroach/issues/41522)');
        }

        $this->expectException(CancelledException::class);

        try {
            $this->conn->waitForNotification(new TimeoutCancellation(0.005));
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testWaitForNotificationPreCancelled(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support LISTEN / NOTIFY (https://github.com/cockroachdb/cockroach/issues/41522)');
        }

        $this->expectException(CancelledException::class);

        $cancellation = new TimeoutCancellation(0.005);
        delay(0.1);

        $this->conn->waitForNotification($cancellation);
    }

    public function testCopyFrom(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not fully support COPY FROM (https://www.cockroachlabs.com/docs/v20.2/copy-from.html)');
        }

        $sql = <<<SQL
create temporary table foo(
    a int4,
    b varchar
)
SQL;

        $this->conn->exec($sql)->readAll();

        $genData = static function (): \Generator {
            for ($i = 0; $i < 1000; $i++) {
                yield "{$i}, \"foo {$i} bar\"\n";
            }
        };
        $stream = new ReadableIterableStream($genData());

        $ct = $this->conn->copyFrom('COPY foo FROM STDIN WITH (FORMAT csv)', $stream);
        self::assertSame(1000, $ct->rowsAffected());

        $result = $this->conn->execParams('SELECT * FROM foo')->getResult();
        self::assertCount(1000, $result->getRows());
    }

    public function testCopyFromCancelled(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support query cancellation (https://github.com/cockroachdb/cockroach/issues/41335)');
        }

        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not fully support COPY FROM (https://www.cockroachlabs.com/docs/v20.2/copy-from.html)');
        }

        $sql = <<<SQL
create temporary table foo(
    a int4,
    b varchar
)
SQL;

        $this->conn->exec($sql)->readAll();

        $genData = static function (): \Generator {
            for ($i = 0; $i < 1000000; $i++) {
                yield "{$i}, \"foo {$i} bar\"\n";

                delay(0.001);
            }
        };
        $stream = new ReadableIterableStream($genData());

        $this->expectException(PgErrorException::class);
        $this->expectExceptionMessage('ERROR: COPY from stdin failed: The operation was cancelled (SQLSTATE 57014)');

        try {
            $this->conn->copyFrom('COPY foo FROM STDIN WITH (FORMAT csv)', $stream, new TimeoutCancellation(0.1));
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testCopyFromPreCancelled(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not fully support COPY FROM (https://www.cockroachlabs.com/docs/v20.2/copy-from.html)');
        }

        $sql = <<<SQL
create temporary table foo(
    a int4,
    b varchar
)
SQL;

        $this->conn->exec($sql)->readAll();

        $genData = static fn(): \Generator => yield "\n";
        $stream = new ReadableIterableStream($genData());

        $this->expectException(CancelledException::class);

        $cancellation = new TimeoutCancellation(0.001);
        delay(0.01);

        try {
            $this->conn->copyFrom('COPY foo FROM STDIN WITH (FORMAT csv)', $stream, $cancellation);
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testCopyFromQuerySyntaxError(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not fully support COPY FROM (https://www.cockroachlabs.com/docs/v20.2/copy-from.html)');
        }

        $sql = <<<SQL
create temporary table foo(
    a int4,
    b varchar
)
SQL;

        $this->conn->exec($sql)->readAll();

        $genData = static fn(): \Generator => yield "\n";
        $stream = new ReadableIterableStream($genData());

        $this->expectException(PgErrorException::class);
        $this->expectExceptionMessage('ERROR: syntax error at or near "CROPY" (SQLSTATE 42601)');

        try {
            $this->conn->copyFrom('CROPY fooooo TO STDOUT', $stream);
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testCopyFromNoTableError(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not fully support COPY FROM (https://www.cockroachlabs.com/docs/v20.2/copy-from.html)');
        }

        $sql = <<<SQL
create temporary table foo(
    a int4,
    b varchar
)
SQL;

        $this->conn->exec($sql)->readAll();

        $genData = static fn(): \Generator => yield "\n";
        $stream = new ReadableIterableStream($genData());

        $ct = $this->conn->copyFrom('COPY foo TO stdout', $stream);
        self::assertSame(0, $ct->rowsAffected());

        $this->ensureConnectionValid();
    }

    public function testCopyFromNoticeReceivedMidStream(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not fully support COPY FROM (https://www.cockroachlabs.com/docs/v20.2/copy-from.html)');
        }

        $sql = <<<SQL
create temporary table sentences(
    t text,
    ts tsvector
)
SQL;

        $this->conn->exec($sql)->readAll();

        $sql = <<<SQL
create function pg_temp.sentences_trigger() returns trigger as $$
begin
  new.ts := to_tsvector(new.t);
    return new;
end
$$ language plpgsql;
SQL;

        $this->conn->exec($sql)->readAll();

        $sql = <<<SQL
create trigger sentences_update before insert on sentences for each row execute procedure pg_temp.sentences_trigger();
SQL;

        $this->conn->exec($sql)->readAll();

        $genData = static function(): \Generator {
            $longSting = \str_repeat('X', 10001);

            for ($i = 0; $i < 1000; $i++) {
                yield "{$longSting}\n";
            }
        };
        $stream = new ReadableIterableStream($genData());

        $ct = $this->conn->copyFrom('COPY sentences(t) FROM STDIN WITH (FORMAT csv)', $stream);
        self::assertSame(1000, $ct->rowsAffected());

        $this->ensureConnectionValid();
    }

    public function testCopyTo(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support COPY TO');
        }

        $sql = <<<SQL
create temporary table foo(
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f date,
		g json,
		h bytea
	)
SQL;

        $this->conn->exec($sql)->readAll();

        $sql = <<<SQL
insert into foo values (0, 1, 2, 'abc', 'efg', '2000-01-01', '{"abc":"def","foo":"bar"}', 'oooo')
SQL;

        $inputBytes = '';

        for ($i = 0; $i < 1000; $i++) {
            $this->conn->exec($sql)->readAll();

            $inputBytes .= "0\t1\t2\tabc\tefg\t2000-01-01\t{\"abc\":\"def\",\"foo\":\"bar\"}\t\\\\x6f6f6f6f\n";
        }

        $stream = new WritableIterableStream(0);

        $recvStr = '';
        async(static function () use ($stream, &$recvStr) {
            foreach ($stream->getIterator() as $value) {
                $recvStr .= $value;
            }
        });

        $ct = $this->conn->copyTo('COPY foo TO STDOUT', $stream);
        self::assertSame(1000, $ct->rowsAffected());

        self::assertSame($inputBytes, $recvStr);

        $this->ensureConnectionValid();
    }

    public function testCopyToQueryError(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support COPY TO');
        }

        $stream = new WritableIterableStream(0);

        $this->expectException(PgErrorException::class);
        $this->expectExceptionMessage('ERROR: syntax error at or near "CROPY" (SQLSTATE 42601)');

        try {
            $this->conn->copyTo('CROPY foo TO STDOUT', $stream);
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testCopyToCancelled(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support query cancellation (https://github.com/cockroachdb/cockroach/issues/41335)');
        }

        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support COPY TO');
        }

        $stream = new WritableIterableStream(0);

        async(static function () use ($stream) {
            foreach ($stream->getIterator() as $value) {
            }
        });

        $this->expectException(PgErrorException::class);
        $this->expectExceptionMessage('ERROR: canceling statement due to user request (SQLSTATE 57014)');

        try {
            $this->conn->copyTo(
                'copy (select *, pg_sleep(0.01) from generate_series(1,1000)) to stdout',
                $stream,
                new TimeoutCancellation(0.01),
            );
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testCopyToPreCancelled(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support COPY TO');
        }

        $stream = new WritableIterableStream(0);

        $this->expectException(CancelledException::class);

        $cancellation = new TimeoutCancellation(0.01);
        delay(0.1);

        try {
            $this->conn->copyTo(
                'copy (select *, pg_sleep(0.01) from generate_series(1,1000)) to stdout',
                $stream,
                $cancellation,
            );
        } finally {
            $this->ensureConnectionValid();
        }
    }

    public function testCancelRequest(): void
    {
        if ($this->conn->getParameterStatus('crdb_version') !== '') {
            $this->markTestSkipped('Server does not support query cancellation (https://github.com/cockroachdb/cockroach/issues/41335)');
        }

        $this->expectException(PgErrorException::class);
        $this->expectExceptionMessage('ERROR: canceling statement due to user request (SQLSTATE 57014)');

        $mrr = $this->conn->exec("select 'Hello, world', pg_sleep(2)");

        delay(0.05);

        $this->conn->cancelRequest();

        while ($mrr->nextResult()) {}
        $mrr->close();

        $this->ensureConnectionValid();
    }

    public function testCloseWhenQueryInProgress(): void
    {
        $this->expectNotToPerformAssertions();

        $res = $this->conn->exec('select n from generate_series(1,10) n');
        $this->conn->close();

        unset($res);
    }
}