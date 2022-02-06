<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config;

use Psr\Log\LoggerInterface;

/**
 * Settings to establish a connection to a PostgreSQL server.
 */
class Config
{
    /**
     * @param array<HostConfig> $hosts Hosts to try to connect to (in order). At least one host required.
     * @param string $user
     * @param string $database
     *
     * @param float $connectTimeout
     *
     * @param array<string, string> $runtimeParams
     * Run-time parameters to set on connection as session default values (e.g. search_path or application_name).
     *
     * @param LoggerInterface|null $logger
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
     *
     * @param int $minReadBufferSize Minimum read buffer size in bytes
     */
    public function __construct(
        private array $hosts,
        private string $user,
        private string $database = '',
        private float $connectTimeout = 2,
        private array $runtimeParams = [],
        private ?LoggerInterface $logger = null,
        private ?BuildFrontendFunc $buildFrontendFunc = null,
        private ?AfterConnectFuncInterface $afterConnectFunc = null,
        private ?ValidateConnectFuncInterface $validateConnectFunc = null,
        private ?NoticeHandlerInterface $onNotice = null,
        private ?NotificationHandlerInterface $onNotification = null,
        private int $minReadBufferSize = 8192,
    ) {
        $this->validateHosts($this->hosts);
        $this->validateMinReadBufferSize($this->minReadBufferSize);

        if ($this->user === '') {
            throw new \InvalidArgumentException('Missing user');
        }
    }

    /**
     * @param array<HostConfig> $hosts
     * @return void
     *
     * @throws \InvalidArgumentException
     */
    private function validateHosts(array $hosts): void
    {
        if ($hosts === []) {
            throw new \InvalidArgumentException('At least one host required');
        }

        foreach ($hosts as $idx => $host) {
            if (!$host instanceof HostConfig) {
                throw new \InvalidArgumentException("Host at index {$idx} must be an instance of HostConfig");
            }
        }
    }

    /**
     * @param int $minReadBufferSize
     * @return void
     *
     * @throws \InvalidArgumentException
     */
    private function validateMinReadBufferSize(int $minReadBufferSize): void
    {
        if ($minReadBufferSize < 1) {
            throw new \InvalidArgumentException('Minimum buffer size must be at least 1 byte');
        }
    }

    /**
     * @return array<HostConfig>
     */
    public function getHosts(): array
    {
        return $this->hosts;
    }

    public function getDatabase(): string
    {
        return $this->database;
    }

    public function getUser(): string
    {
        return $this->user;
    }

    public function getConnectTimeout(): float
    {
        return $this->connectTimeout;
    }

    /**
     * @return array<string, string>
     */
    public function getRuntimeParams(): array
    {
        return $this->runtimeParams;
    }

    public function getLogger(): ?LoggerInterface
    {
        return $this->logger;
    }

    public function getBuildFrontendFunc(): ?BuildFrontendFunc
    {
        return $this->buildFrontendFunc;
    }

    public function getAfterConnectFunc(): ?AfterConnectFuncInterface
    {
        return $this->afterConnectFunc;
    }

    public function getValidateConnectFunc(): ?ValidateConnectFuncInterface
    {
        return $this->validateConnectFunc;
    }

    public function getOnNotice(): ?NoticeHandlerInterface
    {
        return $this->onNotice;
    }

    public function getOnNotification(): ?NotificationHandlerInterface
    {
        return $this->onNotification;
    }

    public function getMinReadBufferSize(): int
    {
        return $this->minReadBufferSize;
    }

    /**
     * @param HostConfig[] $hosts
     * @return Config
     */
    public function setHosts(array $hosts): Config
    {
        $this->validateHosts($hosts);

        $this->hosts = $hosts;
        return $this;
    }

    public function withDatabase(string $database): Config
    {
        $clone = clone $this;
        $clone->database = $database;

        return $clone;
    }

    public function withUser(string $user): Config
    {
        if ($user === '') {
            throw new \InvalidArgumentException('Missing user');
        }

        $clone = clone $this;
        $clone->user = $user;

        return $clone;
    }

    /**
     * @param float $connectTimeout
     * @return Config
     */
    public function withConnectTimeout(float $connectTimeout): Config
    {
        $clone = clone $this;
        $clone->connectTimeout = $connectTimeout;

        return $clone;
    }

    /**
     * @param array<string, string> $runtimeParams
     * @return Config
     */
    public function withRuntimeParams(array $runtimeParams): Config
    {
        $clone = clone $this;
        $clone->runtimeParams = $runtimeParams;

        return $clone;
    }

    public function withRuntimeParam(string $param, string $value): Config
    {
        $clone = clone $this;
        $clone->runtimeParams[$param] = $value;

        return $clone;
    }

    public function withLogger(?LoggerInterface $logger): Config
    {
        $clone = clone $this;
        $clone->logger = $logger;

        return $clone;
    }

    public function withBuildFrontendFunc(?BuildFrontendFunc $buildFrontendFunc): Config
    {
        $clone = clone $this;
        $clone->buildFrontendFunc = $buildFrontendFunc;

        return $clone;
    }

    public function withAfterConnectFunc(?AfterConnectFuncInterface $afterConnectFunc): Config
    {
        $clone = clone $this;
        $clone->afterConnectFunc = $afterConnectFunc;

        return $clone;
    }

    public function withValidateConnectFunc(?ValidateConnectFuncInterface $validateConnectFunc): Config
    {
        $clone = clone $this;
        $clone->validateConnectFunc = $validateConnectFunc;

        return $clone;
    }

    public function withOnNotice(?NoticeHandlerInterface $onNotice): Config
    {
        $clone = clone $this;
        $clone->onNotice = $onNotice;

        return $clone;
    }

    public function withOnNotification(?NotificationHandlerInterface $onNotification): Config
    {
        $clone = clone $this;
        $clone->onNotification = $onNotification;

        return $this;
    }

    public function withMinReadBufferSize(int $minReadBufferSize): Config
    {
        $this->validateMinReadBufferSize($minReadBufferSize);

        $clone = clone $this;
        $clone->minReadBufferSize = $minReadBufferSize;

        return $clone;
    }
}
