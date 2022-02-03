<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config;

use Amp\Socket\ClientTlsContext;
use PhpPg\PgConn\SslMode;

class FallbackConfig
{
    /**
     * @param string $host host (e.g. localhost) or absolute path to unix domain socket directory (e.g. /private/tmp)
     * @param int $port
     * @param ClientTlsContext|null $tlsConfig NULL disables TLS
     * @param SslMode $sslMode
     */
    public function __construct(
        public string $host,
        public int $port,
        public ?ClientTlsContext $tlsConfig = null,
        public SslMode $sslMode = SslMode::DISABLE,
    ) {
        if ($this->port < 1 || $this->port > 65535) {
            throw new \InvalidArgumentException('Port number must be between 1 and 65535');
        }
    }
}
