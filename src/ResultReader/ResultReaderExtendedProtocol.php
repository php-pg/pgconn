<?php

declare(strict_types=1);

namespace PhpPg\PgConn\ResultReader;

use Amp\ByteStream\ClosedException;
use Amp\Cancellation;
use Amp\CancelledException;
use PhpPg\PgConn\Exception\PgErrorException;
use PhpPg\PgConn\PgConn;
use PhpPg\PgProto3\Messages\BackendMessageInterface;
use PhpPg\PgProto3\Messages\CommandComplete;
use PhpPg\PgProto3\Messages\CommandTag;
use PhpPg\PgProto3\Messages\DataRow;
use PhpPg\PgProto3\Messages\EmptyQueryResponse;
use PhpPg\PgProto3\Messages\FieldDescription;
use PhpPg\PgProto3\Messages\NoData;
use PhpPg\PgProto3\Messages\ReadyForQuery;
use PhpPg\PgProto3\Messages\RowDescription;

/**
 * Represents results of one SQL command in Extended Protocol
 */
class ResultReaderExtendedProtocol implements ResultReaderInterface
{
    private bool $closed = false;

    /**
     * @var array<string|null>
     */
    private array $rowValues = [];

    private ?CommandTag $commandTag = null;

    /**
     * @param PgConn $conn
     * @param \Closure $releaseConn
     * @param array<FieldDescription>|null $fieldDescriptions
     * @param Cancellation|null $cancellation
     * @param string|null $cancelCbId
     */
    public function __construct(
        private PgConn $conn,
        private \Closure $releaseConn,
        private ?array $fieldDescriptions = null,
        private ?Cancellation $cancellation = null,
        private ?string $cancelCbId = null,
    ) {
    }

    public function __destruct()
    {
        try {
            $this->close();
        } catch (\Throwable) {
            // silently ignore errors
        }
    }

    /**
     * @return void
     * @throws ClosedException
     * @throws \PhpPg\PgProto3\Exception\ProtoException
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

        try {
            $this->conn->restoreConnectionState($this->cancellation);
        } finally {
            $this->cancellation?->unsubscribe($this->cancelCbId ?? '');
            ($this->releaseConn)();
        }
    }

    /**
     * @return array<?string>
     */
    public function getRowValues(): array
    {
        return $this->rowValues;
    }

    /**
     * @return array<FieldDescription>
     */
    public function getFieldDescriptions(): array
    {
        return $this->fieldDescriptions ?? [];
    }

    public function getCommandTag(): CommandTag
    {
        if ($this->commandTag === null) {
            throw new \LogicException('Keep calling nextRow until it returns false');
        }

        return $this->commandTag;
    }

    /**
     * Fetch all query results
     * WARNING: May consume a large amount of memory (depends on returned rows size)
     *
     * @return Result
     *
     * @throws ClosedException
     * @throws PgErrorException
     * @throws \PhpPg\PgProto3\Exception\ProtoException
     */
    public function getResult(): Result
    {
        if ($this->closed) {
            throw new \LogicException('ResultReader is closed');
        }

        $rowValues = [];
        while ($this->nextRow()) {
            $rowValues[] = $this->rowValues;
        }

        // Handle NoData response
        if ($this->rowValues === [] && $this->commandTag === null) {
            return new Result(
                $this->fieldDescriptions ?? [],
                [],
                new CommandTag('')
            );
        }

        if ($this->commandTag === null) {
            throw new \LogicException('Something went wrong, CommandComplete message is not received');
        }

        return new Result(
            $this->fieldDescriptions ?? [],
            $rowValues,
            $this->commandTag,
        );
    }

    /**
     * Advances the ResultReader to the next row.
     *
     * @return bool true if a row is available
     *
     * @throws ClosedException
     * @throws PgErrorException
     * @throws \PhpPg\PgProto3\Exception\ProtoException
     */
    public function nextRow(): bool
    {
        while (!$this->closed) {
            try {
                $msg = $this->receiveMessage($this->cancellation);
            } catch (CancelledException) {
                /**
                 * From: Postgres Wire Protocol Flow (53.2.7. Canceling Requests in Progress)
                 *
                 * The cancellation signal might or might not have any effect — for example,
                 * if it arrives after the backend has finished processing the query, then it will have no effect.
                 * If the cancellation is effective,
                 * it results in the current command being terminated early with an error message.
                 *
                 * The upshot of all this is that for reasons of both security and efficiency,
                 * the frontend has no direct way to tell whether a cancel request has succeeded.
                 * It must continue to wait for the backend to respond to the query.
                 * Issuing a cancel simply improves the odds that the current query will finish soon,
                 * and improves the odds that it will fail with an error message instead of succeeding.
                 */
                continue;
            }

            switch ($msg::class) {
                case DataRow::class:
                    $this->rowValues = $msg->values;
                    return true;

                case EmptyQueryResponse::class:
                    $this->rowValues = [];
                    return true;
            }
        }

        return false;
    }

    public function readUntilRowDescription(): void
    {
        while (!$this->closed) {
            try {
                $msg = $this->receiveMessage($this->cancellation);
            } catch (CancelledException) {
                continue;
            }

            if ($msg::class === RowDescription::class || $msg::class === NoData::class) {
                break;
            }
        }
    }

    /**
     * @param Cancellation|null $cancellation
     * @return BackendMessageInterface
     *
     * @throws CancelledException
     * @throws ClosedException
     * @throws PgErrorException
     * @throws \PhpPg\PgProto3\Exception\ProtoException
     */
    private function receiveMessage(?Cancellation $cancellation = null): BackendMessageInterface
    {
        if ($this->closed) {
            throw new \LogicException('ResultReader is closed');
        }

        try {
            $msg = $this->conn->receiveMessage($cancellation);
        } catch (PgErrorException $e) {
            $this->closed = true;

            /**
             * In the event of an error, ErrorResponse is issued followed by ReadyForQuery.
             * All further processing of the query string is aborted by ErrorResponse
             * (even if more queries remained in it).
             * Note that this might occur partway through the sequence of messages generated by an individual query.
             */

            if ($e->pgError->severity === 'FATAL') {
                $this->cancellation?->unsubscribe($this->cancelCbId ?? '');
            } else {
                // Finalize reading on Postgres error (put connection back to a valid state)
                $this->conn->restoreConnectionState($cancellation);
            }

            ($this->releaseConn)();

            throw $e;
        } catch (CancelledException $e) {
            $this->cancellation?->unsubscribe($this->cancelCbId ?? '');
            $this->cancellation = null;
            $this->cancelCbId = null;

            throw $e;
        } catch (\Throwable $e) {
            $this->closed = true;
            $this->cancellation?->unsubscribe($this->cancelCbId ?? '');
            ($this->releaseConn)();

            throw $e;
        }

        switch ($msg::class) {
            // This message will only be intercepted if the interface sent the "Describe (P)" message
            case RowDescription::class:
                if ($this->fieldDescriptions === null) {
                    $this->fieldDescriptions = $msg->fields;
                }
                break;

            /**
             * From: Postgres Protocol Flow (53.2.2. Simple Query)
             *
             * Processing of the query string is complete.
             * A separate message is sent to indicate this because the query string might contain multiple SQL commands.
             * (CommandComplete marks the end of processing one SQL command, not the whole string.)
             */
            case CommandComplete::class:
                $this->commandTag = $msg->commandTag;
                break;

            case ReadyForQuery::class:
                $this->closed = true;
                $this->cancellation?->unsubscribe($this->cancelCbId ?? '');
                ($this->releaseConn)();
                break;

            // TODO: Support portal suspended
            /**
             * If Execute terminates before completing the execution of a portal
             * (due to reaching a nonzero result-row count), it will send a PortalSuspended message;
             * the appearance of this message tells the frontend that another
             * Execute should be issued against the same portal to complete the operation.
             */
        }

        return $msg;
    }
}
