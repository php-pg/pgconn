<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Exception;

use PhpPg\PgConn\PgError;

class PgErrorException extends \Exception
{
    public function __construct(
        public PgError $pgError,
    ) {
        parent::__construct(
            "{$this->pgError->severity}: {$this->pgError->message} (SQLSTATE {$this->pgError->sqlState})",
        );
    }
}
