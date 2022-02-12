<?php

declare(strict_types=1);

namespace PhpPg\PgConn;

use Amp\Cancellation;
use PhpPg\PgConn\Config\Config;

interface PgConnectorInterface
{
    /**
     * @param Config $config
     * @param Cancellation|null $cancellation
     * @return PgConn
     *
     * @throws \PhpPg\PgConn\Exception\ConnectException
     */
    public function connect(Config $config, ?Cancellation $cancellation = null): PgConn;
}
