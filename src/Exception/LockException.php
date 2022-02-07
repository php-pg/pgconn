<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Exception;

use PhpPg\PgConn\PgConnStatus;

class LockException extends \Exception
{
    private PgConnStatus $status;

    public function __construct(PgConnStatus $status)
    {
        parent::__construct("Lock error: Connection {$status->name}");

        $this->status = $status;
    }

    public function getStatus(): PgConnStatus
    {
        return $this->status;
    }
}