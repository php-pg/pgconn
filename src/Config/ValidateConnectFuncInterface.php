<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config;

use PhpPg\PgConn\PgConn;

interface ValidateConnectFuncInterface
{
    /**
     * @param PgConn $conn
     * @throws \Throwable
     */
    public function __invoke(PgConn $conn): void;
}
