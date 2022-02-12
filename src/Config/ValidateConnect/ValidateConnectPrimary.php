<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config\ValidateConnect;

use PhpPg\PgConn\Config\ValidateConnectFuncInterface;
use PhpPg\PgConn\Exception\ValidateConnectException;
use PhpPg\PgConn\PgConn;

class ValidateConnectPrimary implements ValidateConnectFuncInterface
{
    public function __invoke(PgConn $conn): void
    {
        $result = $conn->exec('show hot_standby')->readAll()[0];
        $hotStandbyOpt = $result->getRows()[0][0];

        if ($hotStandbyOpt !== 'off') {
            throw new ValidateConnectException('Server must not be in hot standby mode');
        }
    }
}
