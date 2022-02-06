<?php

declare(strict_types=1);

namespace PhpPg\PgConn;

use Amp\ByteStream\ClosedException;
use Amp\ByteStream\StreamException;
use Amp\Cancellation;
use Amp\CancelledException;
use Amp\Socket\ConnectContext;
use Amp\Socket\EncryptableSocket;
use PhpPg\PgConn\Auth\ScramClient;
use PhpPg\PgConn\Config\Config;
use PhpPg\PgConn\Config\HostConfig;
use PhpPg\PgConn\Exception\ConnectException;
use PhpPg\PgConn\Exception\PgErrorException;
use PhpPg\PgConn\Exception\SASLException;
use PhpPg\PgConn\Exception\UnexpectedMessageException;
use PhpPg\PgProto3\ChunkReader;
use PhpPg\PgProto3\Exception\UnknownAuthMessageTypeException;
use PhpPg\PgProto3\Exception\UnknownMessageTypeException;
use PhpPg\PgProto3\Frontend;
use PhpPg\PgProto3\Messages\AuthenticationCleartextPassword;
use PhpPg\PgProto3\Messages\AuthenticationMd5Password;
use PhpPg\PgProto3\Messages\AuthenticationOk;
use PhpPg\PgProto3\Messages\AuthenticationSASL;
use PhpPg\PgProto3\Messages\AuthenticationSASLContinue;
use PhpPg\PgProto3\Messages\AuthenticationSASLFinal;
use PhpPg\PgProto3\Messages\BackendKeyData;
use PhpPg\PgProto3\Messages\BackendMessageInterface;
use PhpPg\PgProto3\Messages\ErrorResponse;
use PhpPg\PgProto3\Messages\FrontendMessageInterface;
use PhpPg\PgProto3\Messages\ParameterStatus;
use PhpPg\PgProto3\Messages\PasswordMessage;
use PhpPg\PgProto3\Messages\ReadyForQuery;
use PhpPg\PgProto3\Messages\SASLInitialResponse;
use PhpPg\PgProto3\Messages\SASLResponse;
use PhpPg\PgProto3\Messages\SSLRequest;
use PhpPg\PgProto3\Messages\StartupMessage;
use Throwable;

use function Amp\Socket\connect;
use function md5;
use function str_starts_with;

class PgConnector implements PgConnectorInterface
{
    /**
     * @param Config $config
     * @param Cancellation|null $cancellation
     * @return PgConn
     *
     * @throws ConnectException
     * @throws Throwable unhandled error
     */
    public function connect(Config $config, ?Cancellation $cancellation = null): PgConn
    {
        $conn = $this->connectHosts($config, $cancellation);

        $afterConnectFunc = $config->getAfterConnectFunc();
        if ($afterConnectFunc !== null) {
            try {
                ($afterConnectFunc)($conn);
            } catch (Throwable $e) {
                try {
                    $conn->close();
                } catch (Throwable) {
                    // ignore errors
                }

                throw new ConnectException('AfterConnect failed', 0, $e);
            }
        }

        return $conn;
    }

    /**
     * @param Config $config
     * @param Cancellation|null $cancellation
     * @return PgConn
     *
     * @throws ConnectException
     * @throws Throwable
     */
    private function connectHosts(Config $config, ?Cancellation $cancellation): PgConn
    {
        $ex = null;

        /**
         * When multiple hosts are specified, or when a single host name is translated to multiple addresses,
         * all the hosts and addresses will be tried in order, until one succeeds.
         * If none of the hosts can be reached, the connection fails.
         * If a connection is established successfully, but authentication fails,
         * the remaining hosts in the list are not tried.
         */
        foreach ($config->getHosts() as $host) {
            try {
                return $this->tryConnect($host, $config, $cancellation);
            } catch (ConnectException $e) {
                $ex = $e;

                $prev = $e->getPrevious();
                if ($prev instanceof PgErrorException) {
                    // Wrong password or database does not exist
                    if ($prev->pgError->sqlState === '28P01' || $prev->pgError->sqlState === '28000') {
                        throw $e;
                    }
                }

                $config->getLogger()?->warning(
                    __METHOD__ . " cannot establish connection to {$host->getHost()}:{$host->getPort()}",
                    ['err' => $e->getPrevious()?->getMessage() ?? $e->getMessage()]
                );
            } catch (Throwable $e) {
                $config->getLogger()?->error(
                    __METHOD__ . " unknown error while connecting to {$host->getHost()}:{$host->getPort()}",
                    ['err' => $e->getPrevious()?->getMessage() ?? $e->getMessage()]
                );

                // Throw any unhandled errors
                throw $e;
            }
        }

        if ($ex === null) {
            throw new \LogicException('Host is not connected and there is no error, probably bug occurred');
        }

        throw $ex;
    }

    /**
     * @param HostConfig $hostConfig
     * @param Config $config
     * @param Cancellation|null $cancellation
     * @return PgConn
     *
     * @throws ConnectException
     */
    private function tryConnect(
        HostConfig $hostConfig,
        Config $config,
        ?Cancellation $cancellation = null,
    ): PgConn {
        $connCtx = (new ConnectContext())
            ->withMaxAttempts(1)
            ->withTlsContext($hostConfig->getTlsConfig()->tlsContext ?? null)
            ->withConnectTimeout($config->getConnectTimeout());

        $host = $hostConfig->getHost();
        $port = $hostConfig->getPort();

        if (str_starts_with($host, '/')) {
            $address = "unix://{$host}.s.PGSQL.{$port}";
        } else {
            $address = "tcp://{$host}:{$port}";
        }

        try {
            $socket = connect(
                $address,
                $connCtx,
                $cancellation,
            );
        } catch (\Amp\Socket\ConnectException | CancelledException $e) {
            throw new ConnectException('Unable to connect to Postgres', 0, $e);
        }

        $buildFrontendFunc = $config->getBuildFrontendFunc();
        if ($buildFrontendFunc !== null) {
            $frontend = ($buildFrontendFunc)(
                new ChunkReader($socket, $config->getMinReadBufferSize()),
                $socket,
                $config->getLogger(),
            );
        } else {
            $frontend = new Frontend(
                new ChunkReader($socket, $config->getMinReadBufferSize()),
                $socket,
                $config->getLogger(),
            );
        }

        $ctx = new ConnectorContext(
            config: $config,
            hostConfig: $hostConfig,
            socket: $socket,
            frontend: $frontend,
            cancellation: $cancellation,
        );

        $this->startTLS($ctx, $hostConfig);

        $startupMsg = new StartupMessage(
            protocolVersion: StartupMessage::PROTOCOL_VERSION_NUMBER,
            parameters: $config->getRuntimeParams(),
        );
        $startupMsg->parameters['user'] = $config->getUser();
        if ($config->getDatabase() !== '') {
            $startupMsg->parameters['database'] = $config->getDatabase();
        }

        $this->sendMessage($ctx, $startupMsg);

        // login phase
        $this->authenticate($ctx);

        $pid = -1;
        $secretKey = -1;
        $parameterStatuses = [];
        $txStatus = '';

        // fetching parameters phase
        $messagesCnt = 0;
        while (true) {
            if (++$messagesCnt > 1000) {
                throw new ConnectException('Connection failed due to infinite loop detected');
            }

            $msg = $this->receiveMessage($ctx);

            switch ($msg::class) {
                case BackendKeyData::class:
                    $pid = $msg->processId;
                    $secretKey = $msg->secretKey;
                    break;

                case ParameterStatus::class:
                    $parameterStatuses[$msg->name] = $msg->value;
                    break;

                case ReadyForQuery::class:
                    $txStatus = $msg->txStatus;
                    $config->getLogger()?->info(__METHOD__ . ' ready for query');
                    break 2;

                default:
                    throw new ConnectException(
                        'Connection failed due to unexpected message received',
                        0,
                        new UnexpectedMessageException($msg::class)
                    );
            }
        }

        $conn = new PgConn(
            socket: $socket,
            config: $config,
            hostConfig: $hostConfig,
            frontend: $frontend,
            pid: $pid,
            secretKey: $secretKey,
            txStatus: $txStatus,
            parameterStatuses: $parameterStatuses,
        );

        $validateConnectFunc = $config->getValidateConnectFunc();
        if ($validateConnectFunc !== null) {
            try {
                ($validateConnectFunc)($conn);
            } catch (Throwable $e) {
                try {
                    $conn->close();
                } catch (Throwable) {
                    // ignore errors
                }

                throw new ConnectException('ValidateConnection failed', 0, $e);
            }
        }

        return $conn;
    }

    /**
     * @param ConnectorContext $ctx
     * @param HostConfig $hostConfig
     * @return void
     *
     * @throws ConnectException
     */
    private function startTLS(ConnectorContext $ctx, HostConfig $hostConfig): void
    {
        $tlsConfig = $hostConfig->getTlsConfig();
        if ($tlsConfig === null) {
            return;
        }

        if (!$ctx->socket instanceof EncryptableSocket) {
            throw new ConnectException('Socket does not support TLS encryption');
        }

        $this->sendMessage($ctx, new SSLRequest());

        $byte = $ctx->socket->read($ctx->cancellation, 1);
        if ($byte === null) {
            throw new ConnectException('Connection closed by the server');
        }

        if ($byte !== 'N' && $byte !== 'S') {
            throw new ConnectException('Server response is invalid for SSLRequest');
        }

        if ($byte !== 'S') {
            // Allow server to refuse TLS
            if ($tlsConfig->sslMode->isServerAllowedToRefuseTls()) {
                $ctx->config->getLogger()?->info(__METHOD__ . ' server refused SSLRequest, TLS is not enabled');

                return;
            }

            throw new ConnectException('Server refused TLS connection');
        }

        try {
            /** @var EncryptableSocket $socket PHPStan */
            $socket = $ctx->socket;
            $socket->setupTls($ctx->cancellation);
        } catch (StreamException | CancelledException $e) {
            throw new ConnectException('TLS initialization error', 0, $e);
        }

        $ctx->config->getLogger()?->info(__METHOD__ . ' TLS enabled');
    }

    /**
     * @param ConnectorContext $ctx
     * @return void
     *
     * @throws ConnectException
     */
    private function authenticate(ConnectorContext $ctx): void
    {
        // prevent infinite recursion
        $iter = 0;
        $maxHops = 5;

        while ($iter < $maxHops) {
            $msg = $this->receiveMessage($ctx);

            switch ($msg::class) {
                case AuthenticationOk::class:
                    $ctx->config->getLogger()?->info(__METHOD__ . ' auth ok');
                    return;

                case AuthenticationCleartextPassword::class:
                    $this->txPasswordMessage($ctx, $ctx->hostConfig->getPassword());
                    break;

                case AuthenticationMd5Password::class:
                    $digestedPassword = "md5" . md5(
                        md5($ctx->hostConfig->getPassword() . $ctx->config->getUser()) . $msg->salt
                    );

                    $this->txPasswordMessage($ctx, $digestedPassword);
                    break;

                case AuthenticationSASL::class:
                    try {
                        $this->handleSasl($ctx, $msg->authMechanisms);
                    } catch (SASLException $e) {
                        throw new ConnectException('Connection failed due to SCRAM protocol flow error', 0, $e);
                    }
                    break;

                default:
                    throw new ConnectException(
                        'Connection failed due to unexpected message received',
                        0,
                        new UnexpectedMessageException($msg::class),
                    );
            }

            $iter++;
        }

        throw new ConnectException('Connection failed due to infinite loop detected during authentication step');
    }

    /**
     * @param ConnectorContext $ctx
     * @param string $password
     * @return void
     *
     * @throws ConnectException
     */
    private function txPasswordMessage(ConnectorContext $ctx, string $password): void
    {
        $passwordMessage = new PasswordMessage(
            password: $password,
        );

        $this->sendMessage($ctx, $passwordMessage);
    }

    /**
     * @param ConnectorContext $ctx
     * @param array<string> $serverAuthMechanisms
     * @return void
     *
     * @throws ConnectException
     * @throws SASLException
     */
    private function handleSasl(
        ConnectorContext $ctx,
        array $serverAuthMechanisms
    ): void {
        $client = new ScramClient(
            $serverAuthMechanisms,
            $ctx->hostConfig->getPassword(),
        );

        $resp = $client->getFirstMessage();

        $firstMsg = new SASLInitialResponse(
            authMechanism: $client->getAlgo(),
            data: $resp,
        );

        $this->sendMessage($ctx, $firstMsg);

        $msg = $this->receiveMessage($ctx);
        if (!$msg instanceof AuthenticationSASLContinue) {
            throw new ConnectException(
                'Connection failed due to unexpected message received',
                0,
                new UnexpectedMessageException($msg::class, 'AuthenticationSASLContinue'),
            );
        }

        $client->recvServerFirstMessage($msg->data);

        $secondMsg = new SASLResponse(
            data: $client->getClientFinalMessage(),
        );

        $this->sendMessage($ctx, $secondMsg);

        $msg = $this->receiveMessage($ctx);
        if (!$msg instanceof AuthenticationSASLFinal) {
            throw new ConnectException(
                'Connection failed due to unexpected message received',
                0,
                new UnexpectedMessageException($msg::class, 'AuthenticationSASLFinal'),
            );
        }

        $client->recvServerFinalMessage($msg->data);
    }

    /**
     * @param ConnectorContext $ctx
     * @return BackendMessageInterface
     *
     * @throws ConnectException
     */
    private function receiveMessage(ConnectorContext $ctx): BackendMessageInterface
    {
        try {
            $msg = $ctx->frontend->receive($ctx->cancellation);
        } catch (ClosedException $e) {
            throw new ConnectException('Connection closed by the server', 0, $e);
        } catch (CancelledException $e) {
            throw new ConnectException('Connection cancelled by user', 0, $e);
        } catch (UnknownMessageTypeException | UnknownAuthMessageTypeException $e) {
            throw new ConnectException('Connection failed due to unknown message received', 0, $e);
        }

        if ($msg::class === ErrorResponse::class) {
            throw new ConnectException(
                'Connection failed due to Postgres error',
                0,
                Internal\getPgErrorExceptionFromMessage($msg)
            );
        }

        return $msg;
    }

    /**
     * @param ConnectorContext $ctx
     * @param FrontendMessageInterface $msg
     * @return void
     *
     * @throws ConnectException
     */
    private function sendMessage(ConnectorContext $ctx, FrontendMessageInterface $msg): void
    {
        try {
            $ctx->frontend->send($msg);
        } catch (ClosedException $e) {
            throw new ConnectException('Connection closed by the server', 0, $e);
        }
    }
}
