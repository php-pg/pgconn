<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Exception;

class UnexpectedMessageException extends ConnectException
{
    private string $receivedMsg;
    private ?string $expectedMsg;

    public function __construct(string $receivedMsg, ?string $expectedMsg = null)
    {
        if ($expectedMsg === null) {
            $msg = \sprintf('Received unexpected message: %s', $receivedMsg);
        } else {
            $msg = \sprintf('Received unexpected message: %s, while expected: %s', $receivedMsg, $expectedMsg);
        }

        parent::__construct($msg);

        $this->receivedMsg = $receivedMsg;
        $this->expectedMsg = $expectedMsg;
    }

    public function getReceivedMsg(): string
    {
        return $this->receivedMsg;
    }

    public function getExpectedMsg(): ?string
    {
        return $this->expectedMsg;
    }
}
