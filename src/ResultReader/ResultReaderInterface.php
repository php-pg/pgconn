<?php

declare(strict_types=1);

namespace PhpPg\PgConn\ResultReader;

use PhpPg\PgProto3\Messages\CommandTag;
use PhpPg\PgProto3\Messages\FieldDescription;

/**
 * Represents results of one SQL command
 */
interface ResultReaderInterface
{
    public function close(): void;

    /**
     * @return array<?string>
     */
    public function getRowValues(): array;

    /**
     * @return array<FieldDescription>
     */
    public function getFieldDescriptions(): array;

    public function getCommandTag(): CommandTag;

    /**
     * Fetch all query results
     *
     * WARNING: May consume a large amount of memory (depends on returned rows size).
     * For less memory consumption use nextRow() + getRowValues() and when nextRow() returns false call getCommandTag().
     *
     * @return Result
     */
    public function getResult(): Result;

    /**
     * Advances the ResultReader to the next row.
     * @return bool true if a row is available
     */
    public function nextRow(): bool;
}
