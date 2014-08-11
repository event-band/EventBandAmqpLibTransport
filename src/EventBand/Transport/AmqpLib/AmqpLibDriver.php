<?php
/*
 * Copyright (c)
 * Kirill chEbba Chebunin <iam@chebba.org>
 *
 * This source file is subject to the MIT license that is bundled
 * with this package in the file LICENSE.
 */

namespace EventBand\Transport\AmqpLib;

use EventBand\Transport\Amqp\Definition\ConnectionDefinition;
use EventBand\Transport\Amqp\Definition\ExchangeDefinition;
use EventBand\Transport\Amqp\Definition\QueueDefinition;
use EventBand\Transport\Amqp\Driver\AmqpDriver;
use EventBand\Transport\Amqp\Driver\CustomAmqpMessage;
use EventBand\Transport\Amqp\Driver\DriverException;
use EventBand\Transport\Amqp\Driver\MessageDelivery;
use EventBand\Transport\Amqp\Driver\MessagePublication;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage as AmqpLibMessage;

/**
 * Description of AmqpLibDriver
 *
 * @author Kirill chEbba Chebunin <iam@chebba.org>
 * @license http://opensource.org/licenses/mit-license.php MIT
 */
class AmqpLibDriver implements AmqpDriver
{
    private $connectionFactory;
    /**
     * @var AbstractConnection
     */
    private $connection;
    /**
     * @var AMQPChannel
     */
    private $channel;

    public function __construct(AmqpConnectionFactory $connectionFactory)
    {
        $this->connectionFactory = $connectionFactory;
    }

    /**
     * Get connection
     *
     * @return AbstractConnection
     */
    protected function getConnection()
    {
        if (!$this->connection) {
            $this->connection = $this->connectionFactory->getConnection();
        }

        return $this->connection;
    }

    /**
     * Get channel
     *
     * @return AMQPChannel
     */
    protected function getChannel()
    {
        if (!$this->channel) {
            $this->channel = $this->getConnection()->channel();
        }

        return $this->channel;
    }

    /**
     * Close channel
     */
    protected function closeChannel()
    {
        $this->channel->close();
        $this->channel = null;
    }

    /**
     * {@inheritDoc}
     */
    public function publish(MessagePublication $publication, $exchange, $routingKey = '')
    {
        try {
            $this->getChannel()->basic_publish(
                AmqpMessageUtils::createAmqpLibMessage($publication->getMessage(), $publication->isPersistent()),
                $exchange,
                $routingKey,
                $publication->isMandatory(),
                $publication->isImmediate()
            );
        } catch (\Exception $e) {
            throw new DriverException('Basic publish error', $e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function consume($queue, callable $callback, $timeout)
    {
        // Zero timeout = do not return, select functions expect null for it
        if ($timeout == 0) {
            $timeout = null;
        }
        try {
            $active = true;
            $channel = $this->getChannel();
            $tag = $channel->basic_consume(
                $queue,
                '',
                false, false, false, false,
                function (AMQPLibMessage $msg) use (&$active, $callback, $channel, $queue) {
                    if (!$callback(AmqpMessageUtils::createDelivery($msg, $queue))) {// Stop consuming
                        $active = false;
                    }
                }
            );

            while ($active) {
                $changedStreams = @$this->getConnection()->select($timeout);
                if (false === $changedStreams) {
                    $error = error_get_last();

                    // On SIGTERM signal $error is null. Ubuntu 14.04, PHP 5.5.9-1ubuntu4.3
                    if ($error === null && version_compare(PHP_VERSION, '5.5.9') != -1) {
                        break;
                    // Check if we got interruption from system call, ex. on signal
                    } else if (stripos($error['message'], 'interrupted system call') !== false) {
                        break;
                    }

                    // public function __construct($message = "", $code = 0, Exception $previous = null) { }
                    throw new \RuntimeException(
                        'Error while waiting on stream',
                        0,
                        new \ErrorException($error['message'], 0, $error['type'], $error['file'], $error['line'])
                    );
                } elseif ($changedStreams > 0) {
                    $channel->wait();
                } else {
                    break;
                }
            }

            // Cancel consumer and close channel
            $channel->basic_cancel($tag);
            $this->closeChannel();
        } catch (\Exception $e) {
            throw new DriverException('Basic consume error', $e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function ack(MessageDelivery $delivery)
    {
        try {
            $this->getChannel()->basic_ack($delivery->getTag());
        } catch (\Exception $e) {
            throw new DriverException('Basic ack error', $e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function reject(MessageDelivery $delivery)
    {
        try {
            $this->getChannel()->basic_reject($delivery->getTag(), true);
        } catch (\Exception $e) {
            throw new DriverException('Basic reject error', $e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function declareExchange(ExchangeDefinition $exchange)
    {
        try {
            $this->getChannel()->exchange_declare(
                $exchange->getName(),
                $exchange->getType(),
                false,
                $exchange->isDurable(),
                $exchange->isAutoDeleted(),
                $exchange->isInternal()
            );
        } catch (\Exception $e) {
            throw new DriverException('Exchange declare error', $e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function bindExchange($target, $source, $routingKey = '')
    {
        try {
            $this->getChannel()->exchange_bind($target, $source, $routingKey);
        } catch (\Exception $e) {
            throw new DriverException(sprintf('Exchange bind error "%s":"%s"->"%s"', $source, $routingKey, $target), $e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function declareQueue(QueueDefinition $queue)
    {
        try {
            $this->getChannel()->queue_declare(
                $queue->getName(),
                false,
                $queue->isDurable(),
                $queue->isExclusive(),
                $queue->isAutoDeleted()
            );
        } catch (\Exception $e) {
            throw new DriverException('Queue declare error', $e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function bindQueue($queue, $exchange, $routingKey = '')
    {
        try {
            $this->getChannel()->queue_bind($queue, $exchange, $routingKey);
        } catch (\Exception $e) {
            throw new DriverException(sprintf('Exchange bind error "%s":"%s"->"%s"', $exchange, $routingKey, $queue), $e);
        }
    }
}
