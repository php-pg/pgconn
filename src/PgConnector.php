<?php

declare(strict_types=1);

namespace PhpPg\PgConn;

use Amp\ByteStream\ClosedException;
use Amp\Cancellation;
use Amp\CancelledException;
use Amp\Socket\ConnectContext;
use Amp\Socket\EncryptableSocket;
use Amp\Socket\SocketException;
use Amp\Socket\TlsException;
use PhpPg\PgConn\Auth\ScramClient;
use PhpPg\PgConn\Config\Config;
use PhpPg\PgConn\Config\FallbackConfig;
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

class PgConnector
{
    public function connect(Config $config, ?Cancellation $cancellation = null): PgConn
    {
        $fallbacks = [
            new FallbackConfig(
                host: $config->host,
                port: $config->port,
                tlsConfig: $config->tlsConfig,
            )
        ];
        \array_push($fallbacks, ...$config->fallbacks);

        $ex = null;
        $conn = null;

        foreach ($fallbacks as $fallback) {
            try {
                $conn = $this->tryConnect($fallback, $config, $cancellation);
                $ex = null;
                // stop cycle on successful connect
                break;
            } catch (ConnectException $e) {
                if (($prev = $e->getPrevious()) instanceof PgErrorException) {
                    /** @var PgErrorException $prev */
                    // Wrong password or Database does not exist
                    if ($prev->pgError->sqlState === '28P01' || $prev->pgError->sqlState === '28000') {
                        throw $e;
                    }
                }

                $config->logger?->warning(
                    __METHOD__ . " cannot establish connection to {$fallback->host}:{$fallback->port}",
                    ['err' => $e->getPrevious()?->getMessage() ?? $e->getMessage()]
                );

                $ex = $e;
            }
        }

        if ($ex !== null) {
            throw $ex;
        }

        if ($config->afterConnectFunc !== null) {
            try {
                ($config->afterConnectFunc)($conn);
            } catch (\Throwable $e) {
                throw new ConnectException('AfterConnect failed', 0, $e);
            }
        }

        return $conn;
    }

    private function tryConnect(
        FallbackConfig $fallbackConfig,
        Config $config,
        ?Cancellation $cancellation = null,
    ): PgConn {
        $connCtx = (new ConnectContext())
            ->withTlsContext($fallbackConfig->tlsConfig)
            ->withConnectTimeout($config->connectTimeout);

        if (\str_starts_with($fallbackConfig->host, '/')) {
            $address = "unix://{$fallbackConfig->host}.s.PGSQL.{$fallbackConfig->port}";
        } else {
            $address = "tcp://{$fallbackConfig->host}:{$fallbackConfig->port}";
        }

        try {
            $socket = \Amp\Socket\connect(
                $address,
                $connCtx,
                $cancellation,
            );
        } catch (\Amp\Socket\ConnectException | \Amp\CancelledException $e) {
            throw new ConnectException('Unable to connect to Postgres', 0, $e);
        }

        if ($config->buildFrontendFunc !== null) {
            $frontend = ($config->buildFrontendFunc)(
                new ChunkReader($socket),
                $socket,
            );
        } else {
            $frontend = new Frontend(
                new ChunkReader($socket),
                $socket,
                $config->logger,
            );
        }

        $ctx = new ConnectorContext(
            config: $config,
            socket: $socket,
            frontend: $frontend,
            cancellation: $cancellation,
        );

        if ($fallbackConfig->tlsConfig !== null) {
            $this->startTLS($ctx, $fallbackConfig);
        }

        $startupMsg = new StartupMessage(
            protocolVersion: StartupMessage::PROTOCOL_VERSION_NUMBER,
            parameters: $config->runtimeParams,
        );
        $startupMsg->parameters['user'] = $config->user;
        if ($config->database !== '') {
            $startupMsg->parameters['database'] = $config->database;
        }

        $this->connectSafeSendMessage($ctx, $startupMsg);

        // login phase
        $this->authenticate($ctx);

        $pid = -1;
        $secretKey = -1;
        $parameterStatuses = [];
        $txStatus = '';

        // fetching parameters phase
        while (true) {
            $msg = $this->connectSafeReceiveMessage($ctx);

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
                    $config->logger?->info(__METHOD__ . ' ready for query');
                    break 2;

                default:
                    $socket->close();

                    throw new ConnectException(
                        'Unexpected message received',
                        0,
                        new UnexpectedMessageException($msg::class)
                    );
            }
        }

        $conn = new PgConn(
            $socket,
            $config,
            $frontend,
            $pid,
            $secretKey,
            $txStatus,
            $parameterStatuses,
        );

        if ($config->validateConnectFunc !== null) {
            try {
                ($config->validateConnectFunc)($conn);
            } catch (\Throwable $e) {
                try {
                    $conn->close();
                } catch (\Throwable) {
                    // ignore errors
                }

                throw new ConnectException('ValidationConnection failed', 0, $e);
            }
        }

        return $conn;
    }

    /**
     * @param ConnectorContext $ctx
     * @return void
     * @throws ConnectException
     */
    private function startTLS(ConnectorContext $ctx, FallbackConfig $fallbackConfig): void
    {
        if (!$ctx->socket instanceof EncryptableSocket) {
            throw new ConnectException('Socket does not support TLS encryption');
        }

        $this->connectSafeSendMessage($ctx, new SSLRequest());

        $byte = $ctx->socket->read($ctx->cancellation, 1);
        if ($byte === null) {
            throw new ConnectException('Connection closed by server');
        }

        if ($byte !== 'N' && $byte !== 'S') {
            throw new ConnectException('Server response is invalid for SSLRequest');
        }

        if ($byte !== 'S') {
            // Allow server to refuse TLS
            if ($fallbackConfig->sslMode->isServerAllowedToRefuseTls()) {
                $ctx->config->logger?->info(__METHOD__ . ' server refused SSLRequest, TLS is not enabled');

                return;
            }

            throw new ConnectException('Server refused TLS connection');
        }

        try {
            /** @var EncryptableSocket $socket PHPStan */
            $socket = $ctx->socket;
            $socket->setupTls($ctx->cancellation);
        } catch (TlsException | SocketException | CancelledException $e) {
            throw new ConnectException('TLS initialization error', 0, $e);
        }

        $ctx->config->logger?->info(__METHOD__ . ' TLS enabled');
    }

    private function authenticate(ConnectorContext $ctx): void
    {
        // prevent infinite recursion
        $iter = 0;
        $maxHops = 5;

        while ($iter < $maxHops) {
            $msg = $this->connectSafeReceiveMessage($ctx);

            switch ($msg::class) {
                case AuthenticationOk::class:
                    $ctx->config->logger?->info(__METHOD__ . ' auth ok');
                    return;

                case AuthenticationCleartextPassword::class:
                    $this->txPasswordMessage($ctx, $ctx->config->password);
                    break;

                case AuthenticationMd5Password::class:
                    $digestedPassword = "md5" . md5(
                        md5($ctx->config->password . $ctx->config->user) . $msg->salt
                    );

                    $this->txPasswordMessage($ctx, $digestedPassword);
                    break;

                case AuthenticationSASL::class:
                    try {
                        $this->handleSasl($ctx, $msg->authMechanisms);
                    } catch (SASLException $e) {
                        $ctx->socket->close();

                        throw new ConnectException('SCRAM protocol error', 0, $e);
                    }
                    break;

                default:
                    $ctx->socket->close();

                    throw new ConnectException(
                        'Unexpected message received',
                        0,
                        new UnexpectedMessageException($msg::class)
                    );
            }

            $iter++;
        }

        $ctx->socket->close();

        throw new ConnectException('Recursion detected on authenticate');
    }

    /**
     * @param ConnectorContext $ctx
     * @param string $password
     * @return void
     */
    private function txPasswordMessage(ConnectorContext $ctx, string $password): void
    {
        $passwordMessage = new PasswordMessage(
            password: $password,
        );

        $this->connectSafeSendMessage($ctx, $passwordMessage);
    }

    /**
     * @param array<string> $serverAuthMechanisms
     * @return void
     * @throws ConnectException
     * @throws SASLException
     */
    private function handleSasl(
        ConnectorContext $ctx,
        array $serverAuthMechanisms
    ): void {
        $client = new ScramClient(
            $serverAuthMechanisms,
            $ctx->config->password,
        );

        $resp = $client->getFirstMessage();

        $firstMsg = new SASLInitialResponse(
            authMechanism: $client->getAlgo(),
            data: $resp,
        );

        $this->connectSafeSendMessage($ctx, $firstMsg);

        $msg = $this->connectSafeReceiveMessage($ctx);
        if (!$msg instanceof AuthenticationSASLContinue) {
            $ctx->socket->close();

            throw new UnexpectedMessageException($msg::class, 'AuthenticationSASLContinue');
        }

        $client->recvServerFirstMessage($msg->data);

        $secondMsg = new SASLResponse(
            data: $client->getClientFinalMessage(),
        );

        $this->connectSafeSendMessage($ctx, $secondMsg);

        $msg = $this->connectSafeReceiveMessage($ctx);
        if (!$msg instanceof AuthenticationSASLFinal) {
            $ctx->socket->close();

            throw new UnexpectedMessageException($msg::class, 'AuthenticationSASLFinal');
        }

        $client->recvServerFinalMessage($msg->data);
    }

    /**
     * @param ConnectorContext $ctx
     * @return BackendMessageInterface
     *
     * @throws CancelledException
     * @throws ClosedException
     * @throws \PhpPg\PgConn\Exception\PgErrorException
     * @throws UnknownMessageTypeException
     * @throws UnknownAuthMessageTypeException
     */
    protected function receiveMessage(ConnectorContext $ctx): BackendMessageInterface
    {
        $message = $ctx->frontend->receive($ctx->cancellation);
        if ($message::class === ErrorResponse::class) {
            throw Internal\getPgErrorExceptionFromMessage($message);
        }

        return $message;
    }

    /**
     * @return BackendMessageInterface
     * @throws ConnectException
     */
    protected function connectSafeReceiveMessage(ConnectorContext $ctx): BackendMessageInterface
    {
        try {
            return $this->receiveMessage($ctx);
        } catch (PgErrorException $e) {
            $ctx->socket->close();

            throw new ConnectException('Connection failed due to Postgres error', 0, $e);
        } catch (ClosedException $e) {
            $ctx->socket->close();

            throw new ConnectException('Connection closed', 0, $e);
        } catch (UnknownMessageTypeException $e) {
            $ctx->socket->close();

            throw new ConnectException('Unknown message received', 0, $e);
        } catch (UnknownAuthMessageTypeException $e) {
            $ctx->socket->close();

            throw new ConnectException('Unknown auth message received', 0, $e);
        } catch (CancelledException $e) {
            $ctx->socket->close();

            throw new ConnectException('Connection cancelled', 0, $e);
        } catch (\Throwable $e) {
            $ctx->socket->close();

            throw $e;
        }
    }

    /**
     * @throws ConnectException
     */
    protected function connectSafeSendMessage(ConnectorContext $ctx, FrontendMessageInterface $msg): void
    {
        try {
            $ctx->frontend->send($msg);
        } catch (ClosedException $e) {
            $msgName = $msg::class;
            throw new ConnectException("Unable to send {$msgName}", 0, $e);
        }
    }
}
