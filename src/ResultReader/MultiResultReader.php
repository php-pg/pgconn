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
use PhpPg\PgProto3\Messages\ReadyForQuery;
use PhpPg\PgProto3\Messages\RowDescription;

/**
 * Represent results of multiple SQL commands in Simple Protocol
 */
class MultiResultReader
{
    private bool $closed = false;
    private ?ResultReaderInterface $reader = null;
    /**
     * @var array<Result>|null
     */
    private ?array $partialResults = null;

    public function __construct(
        private PgConn $conn,
        private \Closure $releaseConn,
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

    public function isClosed(): bool
    {
        return $this->closed;
    }

    /**
     * @return array<Result>
     *
     * @throws ClosedException
     * @throws CancelledException
     * @throws PgErrorException
     */
    public function readAll(): array
    {
        $results = [];

        try {
            while ($this->nextResult()) {
                $results[] = $this->getResultReader()->getResult();
            }
        } catch (\Throwable $e) {
            $this->partialResults = $results;

            throw $e;
        }

        return $results;
    }

    /**
     * Returns partial results from readAll() on error
     *
     * @return array<Result>
     */
    public function getPartialResults(): array
    {
        $results = $this->partialResults;
        if ($results === null) {
            throw new \LogicException('Partial results are only available when readAll() throws an exception');
        }

        $this->partialResults = null;

        return $results;
    }

    /**
     * Advances the MultiResultReader to the next result.
     * Each result represents one SQL command, if multiple commands are specified, multiple results will be available.
     *
     * @return bool true if a result is available
     *
     * @throws ClosedException
     * @throws CancelledException
     * @throws PgErrorException
     */
    public function nextResult(): bool
    {
        /**
         * From: Postgres Protocol Flow (51.2.2. Simple Query)
         *
         * A frontend must be prepared to accept ErrorResponse and NoticeResponse messages
         * whenever it is expecting any other type of message.
         * See also Section 51.2.6 concerning messages that the backend might generate due to outside events.
         *
         * Recommended practice is to code frontends in a state-machine style
         * that will accept any message type at any time that it could make sense,
         * rather than wiring in assumptions about the exact sequence of messages.
         */

        // We will not say that there are no results until we get ReadyForQuery message
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
                case RowDescription::class:
                    $this->reader = new ResultReaderSimpleProtocol(
                        mrr: $this,
                        fieldDescriptions: $msg->fields,
                        cancellation: $this->cancellation,
                    );
                    return true;

                case CommandComplete::class:
                    $this->reader = new ResultReaderSimpleProtocol(
                        mrr: $this,
                        commandTag: $msg->commandTag,
                        cancellation: $this->cancellation,
                    );
                    return true;

                case ReadyForQuery::class:
                    return false;
            }
        }

        return false;
    }

    public function getResultReader(): ResultReaderInterface
    {
        if ($this->reader === null) {
            throw new \LogicException('Call nextResult first');
        }

        return $this->reader;
    }

    /**
     * @param Cancellation|null $cancellation
     * @return BackendMessageInterface
     *
     * @throws CancelledException
     * @throws ClosedException
     * @throws PgErrorException
     * @throws \PhpPg\PgProto3\Exception\ProtoException
     * @internal Do not call in external code, this is low level method to accept Postgres Protocol message.
     * Calling this method in external code may put Postgres connection into broken state.
     */
    public function receiveMessage(?Cancellation $cancellation = null): BackendMessageInterface
    {
        if ($this->closed) {
            throw new \LogicException('MultiResultReader is closed');
        }

        try {
            $message = $this->conn->receiveMessage($cancellation);
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

        /**
         * From: Postgres Protocol Flow (53.2.3. Extended Query)
         * The Describe message (portal variant) specifies the name of an existing portal
         * (or an empty string for the unnamed portal).
         * The response is a RowDescription message describing the rows that will be returned by executing the portal;
         * or a NoData message if the portal does not contain a query that will return rows;
         * or ErrorResponse if there is no such portal.
         */

        switch ($message::class) {
            /**
             * Processing of the query string is complete.
             * A separate message is sent to indicate this because the query string might contain multiple SQL commands.
             * (CommandComplete marks the end of processing one SQL command, not the whole string.)
             * ReadyForQuery will always be sent, whether processing terminates successfully or with an error.
             */
            case ReadyForQuery::class:
                $this->closed = true;
                $this->cancellation?->unsubscribe($this->cancelCbId ?? '');
                ($this->releaseConn)();
                break;
        }

        return $message;
    }
}
