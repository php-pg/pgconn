<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config\ValidateConnect;

use PhpPg\PgConn\Config\ValidateConnectFuncInterface;
use PhpPg\PgConn\Exception\ValidateConnectException;
use PhpPg\PgConn\PgConn;

class ValidateConnectReadOnly implements ValidateConnectFuncInterface
{
    public function __invoke(PgConn $conn): void
    {
        $results = $conn->exec('show hot_standby; show transaction_read_only;')->readAll();

        $hotStandbyOpt = $results[0]->rows[0][0];
        $transactionReadOnlyOpt = $results[1]->rows[0][0];

        if ($hotStandbyOpt !== 'on' && $transactionReadOnlyOpt !== 'on') {
            throw new ValidateConnectException('Server transactions must be read-only and hot standby mode enabled');
        }
    }
}
