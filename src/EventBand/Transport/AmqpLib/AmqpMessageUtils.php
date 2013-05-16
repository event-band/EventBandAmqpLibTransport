<?php
/*
 * Copyright (c)
 * Kirill chEbba Chebunin <iam@chebba.org>
 *
 * This source file is subject to the MIT license that is bundled
 * with this package in the file LICENSE.
 */

namespace EventBand\Transport\AmqpLib;

use EventBand\Transport\Amqp\Driver\AmqpMessage;
use EventBand\Transport\Amqp\Driver\CustomAmqpMessage;
use EventBand\Transport\Amqp\Driver\MessageDelivery;
use PhpAmqpLib\Message\AMQPMessage as AmqpLibMessage;

/**
 * Utils for AMQPMessage driver adoption
 *
 * @author Kirill chEbba Chebunin <iam@chebba.org>
 * @license http://opensource.org/licenses/mit-license.php MIT
 */
final class AmqpMessageUtils
{
    const DELIVERY_MODE_NON_PERSISTENT = 1;
    const DELIVERY_MODE_PERSISTENT = 2;

    private static $PROPERTY_MAP = [
        'headers' => 'application_headers',
        'contentType' => 'content_type',
        'contentEncoding' => 'content_encoding',
        'messageId' => 'message_id',
        'appId' => 'app_id',
        'userId' => 'user_id',
        'priority' => 'priority',
        'timestamp' => 'timestamp',
        'expiration' => 'expiration',
        'type' => 'type',
        'replyTo' => 'reply_to'
    ];

    /**
     * @param AmqpLibMessage $message
     *
     * @return AmqpMessage
     */
    public static function createMessage(AmqpLibMessage $message)
    {
        $properties = ['body' => $message->body];
        foreach (self::$PROPERTY_MAP as $name => $amqpLibName) {
            if ($message->has($amqpLibName)) {
                $properties[$name] = $message->get($amqpLibName);
            }
        }

        return CustomAmqpMessage::fromProperties($properties);
    }

    public static function createAmqpLibMessage(AmqpMessage $message, $persistent = true)
    {
        $amqpLibMessage = new AmqpLibMessage($message->getBody());
        foreach (self::$PROPERTY_MAP as $name => $amqpLibName) {
            $value = $message->{'get'.ucfirst($name)}();
            if ($value !== null) {
                $amqpLibMessage->set($amqpLibName, $value);
            }
        }

        $deliveryMode = $persistent ? self::DELIVERY_MODE_PERSISTENT : self::DELIVERY_MODE_NON_PERSISTENT;
        $amqpLibMessage->set('delivery_mode', $deliveryMode);

        return $amqpLibMessage;
    }

    public static function createDelivery(AmqpLibMessage $msg, $queue)
    {
        return new MessageDelivery(
            AmqpMessageUtils::createMessage($msg),
            $msg->delivery_info['delivery_tag'],
            $msg->delivery_info['exchange'],
            $queue,
            $msg->delivery_info['routing_key'],
            $msg->delivery_info['redelivered']
        );
    }
}
