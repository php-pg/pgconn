<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config;

use Amp\Socket\ClientTlsContext;

class TlsConfig
{
    public function __construct(
        public ClientTlsContext $tlsContext,
        public SslMode $sslMode,
    ) {
    }
}
