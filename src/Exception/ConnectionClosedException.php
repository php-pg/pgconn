<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Exception;

class ConnectionClosedException extends ConnectionException
{
    public function __construct(?\Throwable $previous = null)
    {
        parent::__construct('Connection closed', 0, $previous);
    }
}
