<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config;

class HostConfig
{
    public function __construct(
        private string $host,
        private int $port,
        private string $password,
        private ?TlsConfig $tlsConfig = null,
    ) {
        if ($this->port < 1 || $this->port > 65535) {
            throw new \InvalidArgumentException('Port number must be between 1 and 65535');
        }

        if ($this->host === '') {
            throw new \InvalidArgumentException('Missing host');
        }
    }

    public function getHost(): string
    {
        return $this->host;
    }

    public function getPort(): int
    {
        return $this->port;
    }

    public function getPassword(): string
    {
        return $this->password;
    }

    public function getTlsConfig(): ?TlsConfig
    {
        return $this->tlsConfig;
    }
}
