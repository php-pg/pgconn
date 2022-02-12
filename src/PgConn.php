<?php

declare(strict_types=1);

namespace PhpPg\PgConn;

use Amp\ByteStream\ClosedException;
use Amp\ByteStream\ReadableStream;
use Amp\ByteStream\WritableStream;
use Amp\Cancellation;
use Amp\CancelledException;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use PhpPg\PgConn\Config\Config;
use PhpPg\PgConn\Config\HostConfig;
use PhpPg\PgConn\Exception\ConnectionClosedException;
use PhpPg\PgConn\Exception\PgErrorException;
use PhpPg\PgConn\ResultReader\MultiResultReader;
use PhpPg\PgConn\ResultReader\ResultReaderExtendedProtocol;
use PhpPg\PgProto3\FrontendInterface;
use PhpPg\PgProto3\Messages;

use function Amp\async;

class PgConn
{
    private PgConnStatus $status = PgConnStatus::IDLE;

    public function __construct(
        private Socket $socket,
        private Config $config,
        private HostConfig $hostConfig,
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

    public function getHostConfig(): HostConfig
    {
        return $this->hostConfig;
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

    public function getSocket(): Socket
    {
        return $this->socket;
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
        } catch (\Throwable) {
            // silently ignore any error
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
     * @throws ConnectionClosedException
     * @throws CancelledException
     */
    public function exec(string $sql, ?Cancellation $cancellation = null): MultiResultReader
    {
        $cancellation?->throwIfRequested();

        $this->lock();

        $this->safeSend(new Messages\Query(query: $sql));

        $cancelCbId = $cancellation?->subscribe(function () {
            $this->cancelRequest(new TimeoutCancellation(10));
        });

        return new MultiResultReader($this, $this->unlock(...), $cancellation, $cancelCbId);
    }

    /**
     * Creates a prepared statement. If the name is empty, the anonymous prepared statement will be used.
     *
     * @param string $name
     * @param string $sql
     * @param array<int> $paramOIDs
     * @param Cancellation|null $cancellation
     * @return StatementDescription
     *
     * @throws PgErrorException
     * @throws ConnectionClosedException
     * @throws CancelledException
     */
    public function prepare(
        string $name,
        string $sql,
        array $paramOIDs = [],
        ?Cancellation $cancellation = null,
    ): StatementDescription {
        $cancellation?->throwIfRequested();

        $this->lock();

        $msgs = [];
        $msgs[] = new Messages\Parse(name: $name, query: $sql, parameterOIDs: $paramOIDs);
        $msgs[] = new Messages\Describe(objectType: 'S', name: $name);
        $msgs[] = new Messages\Sync();

        $this->safeSendBulk($msgs);

        $fetchedParamOIDs = [];
        $fetchedFields = [];

        $cancelCbId = $cancellation?->subscribe(function () {
            $this->cancelRequest(new TimeoutCancellation(10));
        });

        try {
            while (true) {
                try {
                    $msg = $this->receiveMessage($cancellation);
                } catch (PgErrorException $e) {
                    $cancellation?->unsubscribe($cancelCbId ?? '');

                    if ($e->pgError->severity !== 'FATAL') {
                        $this->restoreConnectionState();
                    }

                    throw $e;
                } catch (CancelledException) {
                    $cancellation?->unsubscribe($cancelCbId ?? '');
                    $cancellation = null;

                    continue;
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
        } finally {
            $cancellation?->unsubscribe($cancelCbId ?? '');
            $this->unlock();
        }

        return new StatementDescription($name, $sql, $fetchedParamOIDs, $fetchedFields);
    }

    /**
     * Execution of a prepared statement via the PostgreSQL extended query protocol.
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
     * @throws PgErrorException
     * @throws ConnectionClosedException
     * @throws \InvalidArgumentException
     * @throws CancelledException
     */
    public function execPrepared(
        string $stmtName,
        array $paramValues = [],
        array $paramFormats = [],
        array $resultFormats = [],
        ?Cancellation $cancellation = null
    ): ResultReaderExtendedProtocol {
        $this->validateExecParams($paramFormats, $paramValues);

        $cancellation?->throwIfRequested();

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

        $this->safeSendBulk($msgs);

        $cancelCbId = $cancellation?->subscribe(function () {
            $this->cancelRequest(new TimeoutCancellation(10));
        });

        $rr = new ResultReaderExtendedProtocol(
            conn: $this,
            releaseConn: $this->unlock(...),
            // result reader will catch RowDescription message on its own
            fieldDescriptions: null,
            cancellation: $cancellation,
            cancelCbId: $cancelCbId,
        );
        $rr->readUntilRowDescription();

        return $rr;
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
     * @throws PgErrorException
     * @throws ConnectionClosedException
     * @throws \InvalidArgumentException
     * @throws CancelledException
     */
    public function execParams(
        string $sql,
        array $paramValues = [],
        array $paramOIDs = [],
        array $paramFormats = [],
        array $resultFormats = [],
        ?Cancellation $cancellation = null
    ): ResultReaderExtendedProtocol {
        $this->validateExecParams($paramFormats, $paramValues);

        $cancellation?->throwIfRequested();

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

        $this->safeSendBulk($msgs);

        $cancelCbId = $cancellation?->subscribe(function () {
            $this->cancelRequest(new TimeoutCancellation(10));
        });

        $rr = new ResultReaderExtendedProtocol(
            conn: $this,
            releaseConn: $this->unlock(...),
            // result reader will catch RowDescription message on its own
            fieldDescriptions: null,
            cancellation: $cancellation,
            cancelCbId: $cancelCbId,
        );
        $rr->readUntilRowDescription();

        return $rr;
    }

    /**
     * CopyFrom executes the copy command sql and copies all of r to the PostgreSQL server.
     *
     * @param string $sql
     * @param ReadableStream $stream
     * @param Cancellation|null $cancellation
     * @return CommandTag
     *
     * @throws PgErrorException
     * @throws ConnectionClosedException
     * @throws CancelledException
     */
    public function copyFrom(string $sql, ReadableStream $stream, ?Cancellation $cancellation = null): CommandTag
    {
        $cancellation?->throwIfRequested();

        $this->lock();

        $this->safeSend(new Messages\Query($sql));

        $cancelCbId = $cancellation?->subscribe(function () {
            $this->cancelRequest(new TimeoutCancellation(10));
        });

        $commandTag = new CommandTag('');

        // Read until copy in response
        while (true) {
            try {
                $msg = $this->receiveMessage($cancellation);
            } catch (CancelledException) {
                $cancellation = null;
                $cancellation?->unsubscribe($cancelCbId ?? '');

                continue;
            } catch (PgErrorException $e) {
                $cancellation?->unsubscribe($cancelCbId ?? '');

                if ($e->pgError->severity !== 'FATAL') {
                    // Finalize reading on Postgres error (put connection back to a valid state)
                    $this->restoreConnectionState();
                }

                $this->unlock();

                throw $e;
            } catch (\Throwable $e) {
                $cancellation?->unsubscribe($cancelCbId ?? '');
                $this->unlock();

                throw $e;
            }

            if ($msg::class === Messages\CopyInResponse::class) {
                $cancellation?->unsubscribe($cancelCbId ?? '');
                break;
            }

            if ($msg::class === Messages\CommandComplete::class) {
                $commandTag = new CommandTag($msg->commandTag);
                continue;
            }

            if ($msg::class === Messages\ReadyForQuery::class) {
                $cancellation?->unsubscribe($cancelCbId ?? '');

                $this->unlock();

                return $commandTag;
            }
        }

        $cancelCbId = $cancellation?->subscribe(function (CancelledException $e) {
            $this->frontend->send(new Messages\CopyFail($e->getMessage()));
        });

        /** @var \Throwable|null $copyErr */
        $copyErr = null;

        // Send CopyDate in background
        $copyFeature = async(function () use ($stream, &$copyErr, &$cancellation) {
            while ($copyErr === null && null !== ($data = $stream->read($cancellation))) {
                $this->safeSend(new Messages\CopyData($data));
            }

            if ($copyErr === null) {
                // End of stream
                $this->safeSend(new Messages\CopyDone());
            }
        })->catch(function (\Throwable $e) use (&$cancellation, $cancelCbId) {
            if ($e instanceof CancelledException) {
                // CopyFail message is already sent from cancellation subscription
                return;
            }

            $cancellation?->unsubscribe($cancelCbId ?? '');
            $cancellation = null;

            $this->safeSend(new Messages\CopyFail($e->getMessage()));
        });
        $copyFeature->ignore();

        while (true) {
            try {
                $msg = $this->receiveMessage();
            } catch (PgErrorException $e) {
                $copyErr = $e;
                $cancellation?->unsubscribe($cancelCbId ?? '');

                if ($e->pgError->severity !== 'FATAL') {
                    $this->restoreConnectionState();
                }

                $this->unlock();

                throw $e;
            } catch (CancelledException $e) {
                $copyErr = $e;
                $cancellation?->unsubscribe($cancelCbId ?? '');
                $cancellation = null;

                continue;
            } catch (\Throwable $e) {
                $copyErr = $e;
                $cancellation?->unsubscribe($cancelCbId ?? '');
                $this->unlock();

                throw $e;
            }

            if ($msg::class === Messages\CommandComplete::class) {
                $commandTag = new CommandTag($msg->commandTag);
                continue;
            }

            if ($msg::class === Messages\ReadyForQuery::class) {
                break;
            }
        }

        $this->unlock();

        return $commandTag;
    }

    /**
     * CopyTo executes the copy command sql and copies the results to writable stream.
     *
     * @param string $sql
     * @param WritableStream $stream
     * @param Cancellation|null $cancellation
     * @return CommandTag
     *
     * @throws PgErrorException
     * @throws ConnectionClosedException
     * @throws CancelledException
     */
    public function copyTo(string $sql, WritableStream $stream, ?Cancellation $cancellation = null): CommandTag
    {
        $cancellation?->throwIfRequested();

        $this->lock();

        $this->safeSend(new Messages\Query($sql));

        $cancelCbId = $cancellation?->subscribe(function () {
            $this->cancelRequest(new TimeoutCancellation(10));
        });

        $commandTag = new CommandTag('');
        $err = null;

        while (true) {
            try {
                $msg = $this->receiveMessage($cancellation);
            } catch (CancelledException) {
                $cancellation?->unsubscribe($cancelCbId ?? '');
                $cancellation = null;

                continue;
            } catch (PgErrorException $e) {
                $cancellation?->unsubscribe($cancelCbId ?? '');

                if ($e->pgError->severity !== 'FATAL') {
                    // Finalize reading on Postgres error (put connection back to a valid state)
                    $this->restoreConnectionState();
                }

                $this->unlock();

                // err is not null when stream write fails
                if ($err !== null) {
                    throw $err;
                }

                throw $e;
            } catch (\Throwable $e) {
                $cancellation?->unsubscribe($cancelCbId ?? '');

                $this->unlock();

                throw $e;
            }

            if ($msg::class === Messages\CopyData::class) {
                if ($err !== null) {
                    // Do not write data to the stream if it once failed
                    continue;
                }

                try {
                    $stream->write($msg->data);
                } catch (\Throwable $e) {
                    $err = $e;
                    $cancellation?->unsubscribe($cancelCbId ?? '');

                    // Send cancel request on stream write error
                    async(function () {
                        $this->cancelRequest(new TimeoutCancellation(10));
                    })->ignore();
                }

                continue;
            }

            if ($msg::class === Messages\CommandComplete::class) {
                $commandTag = new CommandTag($msg->commandTag);
                continue;
            }

            if ($msg::class === Messages\ReadyForQuery::class) {
                $cancellation?->unsubscribe($cancelCbId ?? '');

                $this->unlock();

                if ($err !== null) {
                    throw $err;
                }

                return $commandTag;
            }
        }
    }

    /**
     * @param Cancellation|null $cancellation
     * @return Notification
     *
     * @throws CancelledException
     * @throws PgErrorException
     * @throws ConnectionClosedException
     */
    public function waitForNotification(?Cancellation $cancellation = null): Notification
    {
        $cancellation?->throwIfRequested();

        $this->lock();

        try {
            while (true) {
                $msg = $this->receiveMessage($cancellation);

                if ($msg::class === Messages\NotificationResponse::class) {
                    return Internal\getNotificationFromMessage($msg);
                }
            }
        } finally {
            $this->unlock();
        }
    }

    /**
     * @param Cancellation|null $cancellation
     * @return void
     * @throws CancelledException
     * @throws ClosedException
     * @throws \Amp\ByteStream\StreamException
     * @throws \Amp\Socket\ConnectException
     */
    public function cancelRequest(?Cancellation $cancellation = null): void
    {
        $cancellation?->throwIfRequested();

        $remote = $this->socket->getRemoteAddress()->toString();
        $sock = \Amp\Socket\connect($remote, null, $cancellation);

        $data = (new Messages\CancelRequest(processId: $this->pid, secretKey: $this->secretKey))->encode();
        $sock->write($data);
        $sock->close();
    }

    /**
     * Receive message from PostgreSQL and perform basic handling
     *
     * @param Cancellation|null $cancellation
     * @return Messages\BackendMessageInterface
     *
     * @throws CancelledException
     * @throws PgErrorException
     * @throws ConnectionClosedException
     *
     * @internal Do not call in external code, this is low level method to accept Postgres Protocol message.
     * Calling this method in external code may break Postgres connection state.
     */
    public function receiveMessage(?Cancellation $cancellation = null): Messages\BackendMessageInterface
    {
        try {
            $msg = $this->frontend->receive($cancellation);
        } catch (CancelledException $e) {
            throw $e;
        } catch (\Throwable $e) {
            /**
             * From: Postgres Protocol Flow (51.2.8. Termination)
             * Closing the connection is also advisable if an unrecognizable message type is received,
             * since this probably indicates loss of message-boundary sync.
             */
            $this->close();

            throw new ConnectionClosedException($e);
        }

        switch ($msg::class) {
            case Messages\ParameterStatus::class:
                $this->parameterStatuses[$msg->name] = $msg->value;
                break;

            case Messages\ReadyForQuery::class:
                $this->txStatus = $msg->txStatus;
                break;

            case Messages\NoticeResponse::class:
                $onNotice = $this->config->getOnNotice();
                if ($onNotice !== null) {
                    ($onNotice)(Internal\getNoticeFromMessage($msg));
                }
                break;

            case Messages\NotificationResponse::class:
                $onNotification = $this->config->getOnNotification();
                if ($onNotification !== null) {
                    ($onNotification)(Internal\getNotificationFromMessage($msg));
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

    /**
     * Restore connection after NON-FATAL error
     *
     * @return void
     *
     * @throws ConnectionClosedException
     *
     * @internal Do not call in external code, this is low level method to accept Postgres Protocol message.
     * Calling this method in external code may break Postgres connection state.
     */
    public function restoreConnectionState(): void
    {
        while (true) {
            try {
                if ($this->receiveMessage()::class === Messages\ReadyForQuery::class) {
                    break;
                }
            } catch (PgErrorException $e) {
                if ($e->pgError->severity === 'FATAL') {
                    // stop on FATAL error
                    break;
                }

                // don't stop on Postgres error exception
                continue;
            } catch (CancelledException) {
                // impossible case, but handle it to prevent unwanted behavior
                continue;
            }
        }
    }

    /**
     * @param Messages\FrontendMessageInterface $msg
     * @return void
     * @throws ConnectionClosedException
     */
    private function safeSend(Messages\FrontendMessageInterface $msg): void
    {
        try {
            $this->frontend->send($msg);
        } catch (\Throwable $e) {
            // Any write errors are fatal
            $this->close();

            throw new ConnectionClosedException($e);
        }
    }

    /**
     * @param array<Messages\FrontendMessageInterface> $msgs
     * @return void
     * @throws ConnectionClosedException
     */
    private function safeSendBulk(array $msgs): void
    {
        try {
            $this->frontend->sendBulk($msgs);
        } catch (\Throwable $e) {
            // Any write errors are fatal
            $this->close();

            throw new ConnectionClosedException($e);
        }
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
     * @throws Exception\LockException
     */
    private function lock(): void
    {
        match ($this->status) {
            PgConnStatus::CLOSED, PgConnStatus::BUSY => throw new Exception\LockException($this->status),
            default => null,
        };

        $this->status = PgConnStatus::BUSY;
    }

    /**
     * @throws Exception\UnlockException
     */
    private function unlock(): void
    {
        if ($this->status === PgConnStatus::CLOSED) {
            // Do nothing when connection is closed
            return;
        }

        if ($this->status !== PgConnStatus::BUSY) {
            throw new Exception\UnlockException($this->status);
        }

        $this->status = PgConnStatus::IDLE;
    }
}
