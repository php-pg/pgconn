<?php

declare(strict_types=1);

namespace PhpPg\PgConn;

abstract class AbstractPgError
{
    public function __construct(
        public string $severity,
        public string $sqlState,
        public string $message,
        public string $detail,
        public string $hint,
        public int $position,
        public int $internalPosition,
        public string $internalQuery,
        public string $where,
        public string $schemaName,
        public string $tableName,
        public string $columnName,
        public string $dataTypeName,
        public string $constraintName,
        public string $file,
        public int $line,
        public string $routine,
    ) {
    }
}
