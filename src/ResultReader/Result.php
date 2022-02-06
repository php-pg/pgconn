<?php

declare(strict_types=1);

namespace PhpPg\PgConn\ResultReader;

use PhpPg\PgProto3\Messages\CommandTag;
use PhpPg\PgProto3\Messages\FieldDescription;

/**
 * Represents result of one completed SQL command
 */
class Result
{
    /**
     * @param array<FieldDescription> $fieldDescriptions
     * @param array<array<int, ?string>> $rows
     * @param CommandTag $commandTag
     */
    public function __construct(
        public array $fieldDescriptions,
        public array $rows,
        public CommandTag $commandTag,
    ) {
    }

    /**
     * @return array<FieldDescription>
     */
    public function getFieldDescriptions(): array
    {
        return $this->fieldDescriptions;
    }

    /**
     * @return array<array<int, ?string>>
     */
    public function getRows(): array
    {
        return $this->rows;
    }

    public function getCommandTag(): CommandTag
    {
        return $this->commandTag;
    }
}
