<?php

declare(strict_types=1);

namespace PhpPg\PgConn;

use Amp\ByteStream\ClosedException;
use Amp\Cancellation;
use Amp\CancelledException;
use Amp\Socket\Socket;
use PhpPg\PgConn\Config\Config;
use PhpPg\PgConn\Exception\PgErrorException;
use PhpPg\PgConn\ResultReader\MultiResultReader;
use PhpPg\PgConn\ResultReader\ResultReaderExtendedProtocol;
use PhpPg\PgProto3\Exception\UnknownAuthMessageTypeException;
use PhpPg\PgProto3\Exception\UnknownMessageTypeException;
use PhpPg\PgProto3\FrontendInterface;
use PhpPg\PgProto3\Messages;

class PgConn
{
    private PgConnStatus $status = PgConnStatus::IDLE;

    public function __construct(
        private Socket $socket,
        private Config $config,
        private FrontendInterface $frontend,
        private int $pid = 0,
        private int $secretKey = 0,
        private string $txStatus = '',
        /**
         * Parameter statuses reported by the server
         *
         * @var array<string, string>
         */
        private array $parameterStatuses = [],
    ) {
    }

    public function __destruct()
    {
        $this->close();
    }

    public function getConfig(): Config
    {
        return $this->config;
    }

    public function getStatus(): PgConnStatus
    {
        return $this->status;
    }

    public function isClosed(): bool
    {
        return $this->status === PgConnStatus::CLOSED;
    }

    public function isBusy(): bool
    {
        return $this->status === PgConnStatus::BUSY;
    }

    public function getPid(): int
    {
        return $this->pid;
    }

    public function getSecretKey(): int
    {
        return $this->secretKey;
    }

    public function getTxStatus(): string
    {
        return $this->txStatus;
    }

    /**
     * @return array<string, string> Parameter statuses reported by the server
     */
    public function getParameterStatuses(): array
    {
        return $this->parameterStatuses;
    }

    /**
     * @param string $key
     * @return string The value of a parameter reported by the server (e.g. server_version).
     * Returns an empty string for unknown parameters.
     */
    public function getParameterStatus(string $key): string
    {
        return $this->parameterStatuses[$key] ?? '';
    }

    public function close(): void
    {
        if ($this->status === PgConnStatus::CLOSED) {
            return;
        }

        $this->status = PgConnStatus::CLOSED;
        if ($this->socket->isClosed()) {
            return;
        }

        try {
            $this->frontend->send(new Messages\Terminate());
        } catch (ClosedException) {
            // silently ignore connection error
        }

        $this->socket->close();
    }

    /**
     * Executes SQL via the PostgreSQL simple query protocol. SQL may contain multiple queries.
     * Execution is implicitly wrapped in a transaction unless a transaction is already in progress or SQL contains
     * transaction control statements.
     *
     * @param string $sql
     * @param Cancellation|null $cancellation
     * @return MultiResultReader
     *
     * @throws ClosedException
     * @throws PgErrorException
     */
    public function query(string $sql, ?Cancellation $cancellation = null): MultiResultReader
    {
        $this->lock();

        $this->frontend->send(new Messages\Query(query: $sql));

        return new MultiResultReader($this, $this->unlock(...), $cancellation);
    }

    /**
     * Creates a prepared statement. If the name is empty, the anonymous prepared statement will be used.
     *
     * @param string $name
     * @param string $sql
     * @param array<int> $paramOIDs
     * @param Cancellation|null $cancellation
     * @return StatementDescription
     */
    public function prepare(
        string $name,
        string $sql,
        array $paramOIDs,
        ?Cancellation $cancellation = null,
    ): StatementDescription {
        $this->lock();
        defer($_, $this->unlock(...));

        $msgs = [];
        $msgs[] = new Messages\Parse(name: $name, query: $sql, parameterOIDs: $paramOIDs);
        $msgs[] = new Messages\Describe(objectType: 'S', name: $name);
        $msgs[] = new Messages\Sync();

        $this->frontend->sendBulk($msgs);

        $fetchedParamOIDs = [];
        $fetchedFields = [];

        while (true) {
            try {
                $msg = $this->receiveMessage($cancellation);
            } catch (PgErrorException $e) {
                // Restore connection to a valid state on non-FATAL error
                if ($e->pgError->severity !== 'FATAL') {
                    $this->restoreConnectionState();
                }

                throw $e;
            }

            switch ($msg::class) {
                case Messages\ParameterDescription::class:
                    $fetchedParamOIDs = $msg->parameterOIDs;
                    break;
                case Messages\RowDescription::class:
                    $fetchedFields = $msg->fields;
                    break;
                case Messages\ReadyForQuery::class:
                    break 2;
            }
        }

        return new StatementDescription($name, $sql, $fetchedParamOIDs, $fetchedFields);
    }

    /**
     * Execution of a prepared statement via the PostgreSQL extended query protocol.
     *
     * @param StatementDescription $stmt
     *
     * @param array<?string> $paramValues are the parameter values.
     * It must be encoded in the format given by paramFormats.
     *
     * @param array<int> $paramFormats is an array of format codes determining for each paramValue column
     * whether it is encoded in text or binary format.
     * If paramFormats is empty all params are text format.
     * execPrepared will fail if count(paramFormats) is not 0, 1, or count(paramValues).
     *
     * @param array<int> $resultFormats is an array of format codes determining for each result column
     * whether it is encoded in text or binary format.
     * If resultFormats is empty all results will be in text format.
     *
     * @param Cancellation|null $cancellation
     *
     * @return ResultReaderExtendedProtocol
     *
     * @throws ClosedException
     * @throws PgErrorException
     * @throws \InvalidArgumentException
     */
    public function executePrepared(
        StatementDescription $stmt,
        array $paramValues,
        array $paramFormats,
        array $resultFormats,
        ?Cancellation $cancellation = null
    ): ResultReaderExtendedProtocol {
        $this->validateExecParams($paramFormats, $paramValues);

        $this->lock();

        $msgs = [];
        $msgs[] = new Messages\Bind(
            preparedStatement: $stmt->name,
            parameterFormatCodes: $paramFormats,
            parameters: $paramValues,
            resultFormatCodes: $resultFormats
        );
        $msgs[] = new Messages\Execute();
        $msgs[] = new Messages\Sync();

        $this->frontend->sendBulk($msgs);

        return new ResultReaderExtendedProtocol(
            conn: $this,
            releaseConn: $this->unlock(...),
            fieldDescriptions: $stmt->fields,
            cancellation: $cancellation,
        );
    }

    /**
     * Execution of a prepared statement via the PostgreSQL extended query protocol.
     *
     * @param array<?string> $paramValues are the parameter values.
     * It must be encoded in the format given by paramFormats.
     *
     * @param array<int> $paramOIDs
     *
     * @param array<int> $paramFormats is an array of format codes determining for each paramValue column
     * whether it is encoded in text or binary format.
     * If paramFormats is empty all params are text format.
     * execPrepared will fail if count(paramFormats) is not 0, 1, or count(paramValues).
     *
     * @param array<int> $resultFormats is an array of format codes determining for each result column
     * whether it is encoded in text or binary format.
     * If resultFormats is empty all results will be in text format.
     *
     * @param Cancellation|null $cancellation
     *
     * @return ResultReaderExtendedProtocol
     *
     * @throws ClosedException
     * @throws PgErrorException
     * @throws \InvalidArgumentException
     */
    public function executeParams(
        string $sql,
        array $paramValues,
        array $paramOIDs,
        array $paramFormats,
        array $resultFormats,
        ?Cancellation $cancellation = null
    ): ResultReaderExtendedProtocol {
        $this->validateExecParams($paramFormats, $paramValues);

        $this->lock();

        $msgs = [];
        $msgs[] = new Messages\Parse(query: $sql, parameterOIDs: $paramOIDs);
        $msgs[] = new Messages\Bind(
            parameterFormatCodes: $paramFormats,
            parameters: $paramValues,
            resultFormatCodes: $resultFormats
        );
        $msgs[] = new Messages\Describe(objectType: 'P');
        $msgs[] = new Messages\Execute();
        $msgs[] = new Messages\Sync();

        $this->frontend->sendBulk($msgs);

        return new ResultReaderExtendedProtocol(
            conn: $this,
            releaseConn: $this->unlock(...),
            // result reader will catch RowDescription message on its own
            fieldDescriptions: null,
            cancellation: $cancellation,
        );
    }

    /**
     * Execution of a prepared statement by name via the PostgreSQL extended query protocol.
     *
     * @param string $stmtName
     *
     * @param array<?string> $paramValues are the parameter values.
     * It must be encoded in the format given by paramFormats.
     *
     * @param array<int> $paramFormats is an array of format codes determining for each paramValue column
     * whether it is encoded in text or binary format.
     * If paramFormats is empty all params are text format.
     * execPrepared will fail if count(paramFormats) is not 0, 1, or count(paramValues).
     *
     * @param array<int> $resultFormats is an array of format codes determining for each result column
     * whether it is encoded in text or binary format.
     * If resultFormats is empty all results will be in text format.
     *
     * @param Cancellation|null $cancellation
     *
     * @return ResultReaderExtendedProtocol
     *
     * @throws ClosedException
     * @throws PgErrorException
     * @throws \InvalidArgumentException
     */
    public function executePreparedByName(
        string $stmtName,
        array $paramValues,
        array $paramFormats,
        array $resultFormats,
        ?Cancellation $cancellation = null,
    ): ResultReaderExtendedProtocol {
        $this->validateExecParams($paramFormats, $paramValues);

        $this->lock();

        $msgs = [];
        $msgs[] = new Messages\Bind(
            preparedStatement: $stmtName,
            parameterFormatCodes: $paramFormats,
            parameters: $paramValues,
            resultFormatCodes: $resultFormats
        );
        $msgs[] = new Messages\Describe(objectType: 'P');
        $msgs[] = new Messages\Execute();
        $msgs[] = new Messages\Sync();

        $this->frontend->sendBulk($msgs);

        return new ResultReaderExtendedProtocol(
            conn: $this,
            // result reader will catch RowDescription message on its own
            releaseConn: $this->unlock(...),
            fieldDescriptions: null,
            cancellation: $cancellation,
        );
    }

    public function waitForNotification(?Cancellation $cancellation = null): Notification
    {
        $this->lock();
        defer($_, $this->unlock(...));

        while (true) {
            $msg = $this->receiveMessage($cancellation);

            if ($msg::class === Messages\NotificationResponse::class) {
                return Internal\getNotificationFromMessage($msg);
            }
        }
    }

    public function cancelRequest(): void
    {
        $remote = $this->socket->getRemoteAddress()->toString();
        $sock = \Amp\Socket\connect($remote);

        $data = (new Messages\CancelRequest(processId: $this->pid, secretKey: $this->secretKey))->encode();
        $sock->write($data);
        $sock->close();
    }

    /**
     * @param array<?string> $paramValues are the parameter values.
     * It must be encoded in the format given by paramFormats.
     *
     * @param array<int> $paramFormats is an array of format codes determining for each paramValue column
     * whether it is encoded in text or binary format.
     * If paramFormats is empty all params are text format.
     * execPrepared will fail if count(paramFormats) is not 0, 1, or count(paramValues).
     * @return void
     * @throws \InvalidArgumentException
     */
    private function validateExecParams(array $paramFormats, array $paramValues): void
    {
        if (($paramsCnt = \count($paramValues)) > 65535) {
            throw new \InvalidArgumentException('Extended protocol limited to 65535 parameters');
        }

        $paramFormatCnt = \count($paramFormats);
        if ($paramFormatCnt > 1 && $paramFormatCnt !== $paramsCnt) {
            throw new \InvalidArgumentException('paramFormats count must be 0, 1 or count(paramValues)');
        }
    }

    /**
     * @return Messages\BackendMessageInterface
     *
     * @throws ClosedException
     * @throws CancelledException
     * @throws UnknownMessageTypeException
     * @throws UnknownAuthMessageTypeException
     * @throws PgErrorException
     *
     * @internal Do not call in external code, this is low level method to accept Postgres Protocol message.
     * Calling this method in external code may put Postgres connection into broken state.
     *
     */
    public function receiveMessage(?Cancellation $cancellation = null): Messages\BackendMessageInterface
    {
        try {
            $msg = $this->frontend->receive($cancellation);
        } catch (ClosedException | UnknownMessageTypeException | UnknownAuthMessageTypeException $e) {
            /**
             * From: Postgres Protocol Flow (51.2.8. Termination)
             * Closing the connection is also advisable if an unrecognizable message type is received,
             * since this probably indicates loss of message-boundary sync.
             */
            $this->close();

            throw $e;
        }

        switch ($msg::class) {
            case Messages\ParameterStatus::class:
                $this->parameterStatuses[$msg->name] = $msg->value;
                break;

            case Messages\ReadyForQuery::class:
                $this->txStatus = $msg->txStatus;
                break;

            case Messages\NoticeResponse::class:
                if ($this->config->onNotice !== null) {
                    ($this->config->onNotice)(Internal\getNoticeFromMessage($msg));
                }
                break;

            case Messages\NotificationResponse::class:
                if ($this->config->onNotification !== null) {
                    ($this->config->onNotification)(Internal\getNotificationFromMessage($msg));
                }
                break;

            case Messages\ErrorResponse::class:
                // Connection is already broken
                if ($msg->getSeverity() === 'FATAL') {
                    $this->close();
                }

                throw Internal\getPgErrorExceptionFromMessage($msg);
        }

        return $msg;
    }

    private function lock(): void
    {
        match ($this->status) {
            PgConnStatus::CLOSED => throw new \LogicException('Lock error: Connection closed'),
            PgConnStatus::BUSY => throw new \LogicException('Lock error: Connection busy'),
            default => null,
        };

        $this->status = PgConnStatus::BUSY;
    }

    private function unlock(): void
    {
        if ($this->status === PgConnStatus::CLOSED) {
            // Do nothing when connection is closed
            return;
        }

        if ($this->status !== PgConnStatus::BUSY) {
            throw new \LogicException('Unlock error: Cannot unlock not busy connection');
        }

        $this->status = PgConnStatus::IDLE;
    }

    private function restoreConnectionState(): void
    {
        try {
            // TODO: Respect cancellation?
            while ($this->receiveMessage()::class !== Messages\ReadyForQuery::class) {
            }
        } catch (\Throwable) {
            // ignore exception
        }
    }
}
