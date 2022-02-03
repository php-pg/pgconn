<?php

declare(strict_types=1);

namespace PhpPg\PgConn;

use PhpPg\PgProto3\Messages\FieldDescription;

class StatementDescription
{
    /**
     * @param string $name
     * @param string $sql
     * @param array<int> $paramOIDs
     * @param array<FieldDescription> $fields
     */
    public function __construct(
        public string $name,
        public string $sql,
        public array $paramOIDs,
        public array $fields,
    ) {
    }
}
