<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Auth;

use PhpPg\PgConn\Exception\SASLException;

use function array_intersect;
use function array_key_exists;
use function base64_decode;
use function base64_encode;
use function count;
use function explode;
use function hash;
use function hash_equals;
use function hash_hmac;
use function hash_pbkdf2;
use function implode;
use function in_array;
use function random_bytes;
use function sprintf;
use function str_split;
use function str_starts_with;
use function substr;

class ScramClient
{
    protected const CLIENT_NONCE_LEN = 18;

    protected const SUPPORTED_ALGOS = [
        'SCRAM-SHA-256',
    ];

    private string $algo;
    private string $pass;
    private string $nonce;
    private string $clientFirstMessageBare;

    private string $serverFirstMessage;
    private string $clientAndServerNonce;
    private string $salt;
    private int $iterations;

    private string $saltedPassword;
    private string $authMessage;

    /**
     * @param array<string> $hashAlgos
     * @param string $pass
     */
    public function __construct(array $hashAlgos, string $pass)
    {
        $foundAlgos = array_intersect(self::SUPPORTED_ALGOS, $hashAlgos);
        if ($foundAlgos === []) {
            throw new SASLException(sprintf('Hash algo %s is not supported', implode(',', $hashAlgos)));
        }

        $this->pass = $pass;
        $this->algo = $foundAlgos[0];
        $this->nonce = base64_encode(random_bytes(self::CLIENT_NONCE_LEN));
    }

    public function getFirstMessage(): string
    {
        $this->clientFirstMessageBare = sprintf(
            'n=,r=%s',
            $this->nonce,
        );

        return sprintf(
            'n,,%s',
            $this->clientFirstMessageBare,
        );
    }

    public function recvServerFirstMessage(string $data): void
    {
        $this->serverFirstMessage = $data;

        $parts = explode(',', $data);
        $decodedParts = [];
        foreach ($parts as $part) {
            if ($part[1] !== '=') {
                throw new SASLException(
                    'invalid SCRAM server-first-message received from server: bad format'
                );
            }

            if (in_array($part[0], ['r', 's', 'i'], true)) {
                $decodedParts[$part[0]] = substr($part, 2);
            }
        }

        if (!array_key_exists('r', $decodedParts)) {
            throw new SASLException(
                'invalid SCRAM server-first-message received from server: did not include r='
            );
        }

        if (!str_starts_with($decodedParts['r'], $this->nonce)) {
            throw new SASLException(
                'invalid SCRAM salt received from server: server SCRAM-SHA-256 nonce is not prefixed by client nonce'
            );
        }

        $this->clientAndServerNonce = $decodedParts['r'];

        if (!array_key_exists('s', $decodedParts)) {
            throw new SASLException(
                'invalid SCRAM server-first-message received from server: did not include s='
            );
        }

        $salt = base64_decode($decodedParts['s'], true);
        if (false === $salt) {
            throw new SASLException('invalid SCRAM salt received from server: unable to decode base64');
        }

        $this->salt = $salt;

        if (!array_key_exists('i', $decodedParts)) {
            throw new SASLException(
                'invalid SCRAM server-first-message received from server: did not include i='
            );
        }

        $iter = (int)$decodedParts['i'];
        if ($iter <= 0) {
            throw new SASLException(
                'invalid SCRAM iteration count received from server: must be a positive number'
            );
        }

        $this->iterations = $iter;
    }

    public function getClientFinalMessage(): string
    {
        $clientMsgWoProof = sprintf('c=biws,r=%s', $this->clientAndServerNonce);

        $this->saltedPassword = hash_pbkdf2('sha256', $this->pass, $this->salt, $this->iterations, 32, true);
        $this->authMessage = $this->clientFirstMessageBare . ',' . $this->serverFirstMessage . ',' . $clientMsgWoProof;

        $proof = $this->computeClientProof($this->saltedPassword, $this->authMessage);

        return sprintf('%s,p=%s', $clientMsgWoProof, $proof);
    }

    public function recvServerFinalMessage(string $data): void
    {
        if (!str_starts_with($data, 'v=')) {
            throw new SASLException('invalid SCRAM server-final-message received from server');
        }

        $serverSign = substr($data, 2);
        $computedServerSign = $this->computeServerSignature($this->saltedPassword, $this->authMessage);

        if (!hash_equals($computedServerSign, $serverSign)) {
            throw new SASLException('invalid SCRAM ServerSignature received from server');
        }
    }

    protected function computeClientProof(string $saltedPassword, string $authMessage): string
    {
        $clientKey = hash_hmac('sha256', 'Client Key', $saltedPassword, true);
        $storedKey = hash('sha256', $clientKey, true);

        $clientSignature = hash_hmac('sha256', $authMessage, $storedKey, true);

        $clientProof = [];

        $clientKeyArr = str_split($clientKey);
        $clientSignArr = str_split($clientSignature);

        for ($i = 0, $iMax = count($clientSignArr); $i < $iMax; $i++) {
            $clientProof[] = $clientKeyArr[$i] ^ $clientSignArr[$i];
        }

        $clientProofStr = implode('', $clientProof);

        return base64_encode($clientProofStr);
    }

    protected function computeServerSignature(string $saltedPassword, string $authMessage): string
    {
        $serverKey = hash_hmac('sha256', 'Server Key', $saltedPassword, true);
        $serverSign = hash_hmac('sha256', $authMessage, $serverKey, true);

        return base64_encode($serverSign);
    }

    public function getAlgo(): string
    {
        return $this->algo;
    }
}
