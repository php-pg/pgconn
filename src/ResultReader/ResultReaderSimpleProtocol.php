<?php

declare(strict_types=1);

namespace PhpPg\PgConn\ResultReader;

use Amp\ByteStream\ClosedException;
use Amp\Cancellation;
use Amp\CancelledException;
use PhpPg\PgConn\Exception\PgErrorException;
use PhpPg\PgProto3\Messages\BackendMessageInterface;
use PhpPg\PgProto3\Messages\CommandComplete;
use PhpPg\PgProto3\Messages\CommandTag;
use PhpPg\PgProto3\Messages\DataRow;
use PhpPg\PgProto3\Messages\FieldDescription;

/**
 * Represents results of one SQL command in Simple Protocol
 */
class ResultReaderSimpleProtocol implements ResultReaderInterface
{
    private bool $closed = false;
    private bool $commandTagOnly = false;

    /**
     * @var array<string|null>
     */
    private array $rowValues = [];

    /**
     * When $rowDescription is not null - RowDescription (rows available)
     * When $commandTag is not null - CommandComplete (no rows available)
     *
     * @param array<FieldDescription>|null $fieldDescriptions
     */
    public function __construct(
        private MultiResultReader $mrr,
        private ?array $fieldDescriptions = null,
        private ?CommandTag $commandTag = null,
        private ?Cancellation $cancellation = null,
    ) {
        if ($this->fieldDescriptions === null && $this->commandTag === null) {
            throw new \LogicException('fieldDescriptions and commandTag could not be null');
        }

        if ($this->commandTag !== null) {
            $this->closed = true;
            $this->commandTagOnly = true;
        }
    }

    public function __destruct()
    {
        try {
            $this->close();
        } catch (\Throwable) {
            // silently ignore errors
        }
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

        // read remaining messages
        $this->restoreConnectionState();
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
     * @throws CancelledException
     * @throws PgErrorException
     */
    public function getResult(): Result
    {
        if ($this->closed && !$this->commandTagOnly) {
            throw new \LogicException('ResultReader is closed');
        }

        $rowValues = [];
        while ($this->nextRow()) {
            $rowValues[] = $this->rowValues;
        }

        if ($this->commandTag === null) {
            throw new \LogicException(
                'Something went wrong, commandTag is null, CommandCompleted message is not caught'
            );
        }

        return new Result(
            $this->fieldDescriptions ?? [],
            $rowValues,
            $this->commandTag,
        );
    }

    /**
     * Advances the ResultReader to the next row.
     * @return bool true if a row is available
     *
     * @throws ClosedException
     * @throws CancelledException
     * @throws PgErrorException
     */
    public function nextRow(): bool
    {
        while (!$this->closed) {
            $msg = $this->receiveMessage($this->cancellation);

            switch ($msg::class) {
                case DataRow::class:
                    $this->rowValues = $msg->values;
                    return true;
            }
        }

        return false;
    }

    /**
     * @return BackendMessageInterface
     *
     * @throws ClosedException
     * @throws CancelledException
     * @throws PgErrorException
     */
    private function receiveMessage(?Cancellation $cancellation = null): BackendMessageInterface
    {
        if ($this->closed) {
            throw new \LogicException('ResultReader is closed');
        }

        try {
            $msg = $this->mrr->receiveMessage($cancellation);
        } catch (\Throwable $e) {
            $this->closed = true;

            throw $e;
        }

        switch ($msg::class) {
            /**
             * From: Postgres Protocol Flow (53.2.2. Simple Query)
             *
             * Processing of the query string is complete.
             * A separate message is sent to indicate this because the query string might contain multiple SQL commands.
             * (CommandComplete marks the end of processing one SQL command, not the whole string.)
             */
            case CommandComplete::class:
                $this->closed = true;
                $this->commandTag = $msg->commandTag;
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

    private function restoreConnectionState(): void
    {
        while (true) {
            try {
                if ($this->mrr->receiveMessage($this->cancellation)::class === CommandComplete::class) {
                    break;
                }
            } catch (\Throwable) {
                // Stop on any error, any further connection state restore logic will do the MultiResultReader
                break;
            }
        }
    }
}
