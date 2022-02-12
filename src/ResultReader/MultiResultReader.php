<?php

declare(strict_types=1);

namespace PhpPg\PgConn\ResultReader;

use Amp\Cancellation;
use Amp\CancelledException;
use PhpPg\PgConn\CommandTag;
use PhpPg\PgConn\Exception\ConnectionClosedException;
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
            // silently ignore any errors
        }
    }

    /**
     * @return void
     * @throws ConnectionClosedException
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        $this->unsubscribeCancellation();

        try {
            $this->conn->restoreConnectionState();
        } finally {
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
     * @throws ConnectionClosedException
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
     * @throws ConnectionClosedException
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
                $msg = $this->receiveMessage();
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
                    );
                    return true;

                case CommandComplete::class:
                    $this->reader = new ResultReaderSimpleProtocol(
                        mrr: $this,
                        commandTag: new CommandTag($msg->commandTag),
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
     * @return BackendMessageInterface
     *
     * @throws CancelledException
     * @throws ConnectionClosedException
     * @throws PgErrorException
     *
     * @internal Do not call in external code, this is low level method to accept Postgres Protocol message.
     * Calling this method in external code may put Postgres connection into broken state.
     */
    public function receiveMessage(): BackendMessageInterface
    {
        if ($this->closed) {
            throw new \LogicException('MultiResultReader is closed');
        }

        try {
            $msg = $this->conn->receiveMessage($this->cancellation);
        } catch (PgErrorException $e) {
            $this->closed = true;
            $this->unsubscribeCancellation();

            /**
             * In the event of an error, ErrorResponse is issued followed by ReadyForQuery.
             * All further processing of the query string is aborted by ErrorResponse
             * (even if more queries remained in it).
             * Note that this might occur partway through the sequence of messages generated by an individual query.
             */

            if ($e->pgError->severity !== 'FATAL') {
                // Finalize reading on Postgres error (put connection back to a valid state)
                $this->conn->restoreConnectionState();
            }

            ($this->releaseConn)();

            throw $e;
        } catch (CancelledException $e) {
            $this->unsubscribeCancellation();

            throw $e;
        } catch (\Throwable $e) {
            $this->closed = true;
            $this->unsubscribeCancellation();
            ($this->releaseConn)();

            throw $e;
        }

        /**
         * From: Postgres Protocol Flow (53.2.2. Simple Query)
         * Processing of the query string is complete.
         * ReadyForQuery will always be sent, whether processing terminates successfully or with an error.
         */
        if ($msg::class === ReadyForQuery::class) {
            $this->closed = true;
            $this->unsubscribeCancellation();
            ($this->releaseConn)();
        }

        return $msg;
    }

    private function unsubscribeCancellation(): void
    {
        if ($this->cancellation === null) {
            return;
        }

        $this->cancellation->unsubscribe($this->cancelCbId ?? '');
        $this->cancellation = null;
        $this->cancelCbId = null;
    }
}
