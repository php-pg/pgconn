<?php

declare(strict_types=1);

namespace PhpPg\PgConn\ResultReader;

use PhpPg\PgProto3\Messages\CommandTag;
use PhpPg\PgProto3\Messages\FieldDescription;

class Result
{
    /**
     * Result constructor.
     * @param array<FieldDescription> $fieldDescriptions
     * @param array<array<int, string|null>> $rows
     * @param CommandTag $commandTag
     */
    public function __construct(
        public array $fieldDescriptions,
        public array $rows,
        public CommandTag $commandTag,
    ) {
    }
}
