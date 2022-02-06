<?php

declare(strict_types=1);

namespace PhpPg\PgConn;

use Amp\Cancellation;
use Amp\Socket\Socket;
use PhpPg\PgConn\Config\Config;
use PhpPg\PgConn\Config\HostConfig;
use PhpPg\PgProto3\FrontendInterface;

class ConnectorContext
{
    public function __construct(
        public Config $config,
        public HostConfig $hostConfig,
        public Socket $socket,
        public FrontendInterface $frontend,
        public ?Cancellation $cancellation = null,
    ) {
    }
}
