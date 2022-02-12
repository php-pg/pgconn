<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Internal;

use PhpPg\PgConn\Exception\PgErrorException;
use PhpPg\PgConn\Notice;
use PhpPg\PgConn\Notification;
use PhpPg\PgConn\PgError;
use PhpPg\PgProto3\Messages\ErrorResponse;
use PhpPg\PgProto3\Messages\NoticeResponse;
use PhpPg\PgProto3\Messages\NotificationResponse;

function getPgErrorFromMessage(ErrorResponse $msg): PgError
{
    return new PgError(
        severity: $msg->getSeverity(),
        sqlState: $msg->code,
        message: $msg->message,
        detail: $msg->detail,
        hint: $msg->hint,
        position: $msg->position,
        internalPosition: $msg->internalPosition,
        internalQuery: $msg->internalQuery,
        where: $msg->where,
        schemaName: $msg->schemaName,
        tableName: $msg->tableName,
        columnName: $msg->columnName,
        dataTypeName: $msg->dataTypeName,
        constraintName: $msg->constraintName,
        file: $msg->file,
        line: $msg->line,
        routine: $msg->routine,
    );
}

function getPgErrorExceptionFromMessage(ErrorResponse $msg): PgErrorException
{
    return new PgErrorException(getPgErrorFromMessage($msg));
}

function getNoticeFromMessage(NoticeResponse $msg): Notice
{
    return new Notice(
        severity: $msg->getSeverity(),
        sqlState: $msg->code,
        message: $msg->message,
        detail: $msg->detail,
        hint: $msg->hint,
        position: $msg->position,
        internalPosition: $msg->internalPosition,
        internalQuery: $msg->internalQuery,
        where: $msg->where,
        schemaName: $msg->schemaName,
        tableName: $msg->tableName,
        columnName: $msg->columnName,
        dataTypeName: $msg->dataTypeName,
        constraintName: $msg->constraintName,
        file: $msg->file,
        line: $msg->line,
        routine: $msg->routine,
    );
}

function getNotificationFromMessage(NotificationResponse $msg): Notification
{
    return new Notification($msg->pid, $msg->channel, $msg->payload);
}
