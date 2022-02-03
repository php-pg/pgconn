<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config;

use Amp\Socket\ClientTlsContext;
use PhpPg\PgConn\SslMode;
use Psr\Log\LoggerInterface;

/**
 * Settings to establish a connection to a PostgreSQL server.
 */
class Config
{
    /**
     * @param string $host host (e.g. localhost) or absolute path to unix domain socket directory (e.g. /private/tmp)
     * @param int $port
     * @param string $database
     * @param string $user
     * @param string $password
     * @param ClientTlsContext|null $tlsConfig NULL disables TLS
     * @param float $connectTimeout
     * @param LoggerInterface|null $logger
     *
     * @param array<string, string> $runtimeParams
     * Run-time parameters to set on connection as session default values (e.g. search_path or application_name).
     *
     * @param array<FallbackConfig> $fallbacks
     *
     * @param BuildFrontendFunc|null $buildFrontendFunc provide a custom frontend for PgConn
     *
     * @param AfterConnectFuncInterface|null $afterConnectFunc
     * Is called during a connection attempt after a successful authentication with the PostgreSQL server.
     * It can be used to validate that the server is acceptable.
     * If this throws an error the connection is closed and the next fallback config is tried.
     * This allows implementing high availability behavior such as libpq does with target_session_attrs.
     *
     * @param ValidateConnectFuncInterface|null $validateConnectFunc
     * is called after AfterConnect.
     * It can be used to set up the connection (e.g. Set session variables or prepare statements).
     * If this throws an error the connection attempt fails.
     *
     * @param NoticeHandlerInterface|null $onNotice is a callback function called when a notice response is received.
     *
     * @param NotificationHandlerInterface|null $onNotification
     * is a callback function called when a notification from the LISTEN/NOTIFY system is received.
     */
    public function __construct(
        public string $host = '127.0.0.1',
        public int $port = 5432,
        public string $database = '',
        public string $user = '',
        public string $password = '',
        public ?ClientTlsContext $tlsConfig = null,
        public SslMode $sslMode = SslMode::DISABLE,
        public float $connectTimeout = 2,
        public ?LoggerInterface $logger = null,
        public array $runtimeParams = [],
        public array $fallbacks = [],
        public ?BuildFrontendFunc $buildFrontendFunc = null,
        public ?AfterConnectFuncInterface $afterConnectFunc = null,
        public ?ValidateConnectFuncInterface $validateConnectFunc = null,
        public ?NoticeHandlerInterface $onNotice = null,
        public ?NotificationHandlerInterface $onNotification = null,
    ) {
        if ($this->port < 1 || $this->port > 65535) {
            throw new \InvalidArgumentException('Port number must be between 1 and 65535');
        }

        if ($this->host === '') {
            throw new \InvalidArgumentException('Missing host');
        }

        if ($this->database === '') {
            throw new \InvalidArgumentException('Missing database');
        }

        if ($this->user === '') {
            throw new \InvalidArgumentException('Missing user');
        }
    }
}
