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
use EventBand\Transport\Amqp\Driver\DriverException;
use EventBand\Transport\Amqp\Driver\MessageDelivery;
use EventBand\Transport\Amqp\Driver\MessagePublication;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPSocketConnection;
use PhpAmqpLib\Message\AMQPMessage as AmqpLibMessage;

/**
 * Description of AmqpLibDriver
 *
 * @author Kirill chEbba Chebunin <iam@chebba.org>
 * @license http://opensource.org/licenses/mit-license.php MIT
 */
class AmqpLibDriver implements AmqpDriver
{
    /**
     * @var ConnectionDefinition
     */
    private $definition;

    /**
     * @var AMQPSocketConnection
     */
    private $connection;

    /**
     * @var AMQPChannel
     */
    private $channel;

    /**
     * @var bool
     */
    private $closed = false;

    /**
     * @param ConnectionDefinition $definition
     */
    public function __construct(ConnectionDefinition $definition)
    {
        $this->definition = $definition;
    }

    /**
     * @return bool
     */
    public function connect()
    {
        if ($this->isConnected()) {
            throw new DriverException('This driver is already connected.');
        }

        try {
            $this->connection = $this->getConnection();
        } catch (DriverException $exception) {
            return false;
        }

        return true;
    }

    /**
     * @return bool
     */
    public function isConnected()
    {
        return $this->connection !== null;
    }

    /**
     * @return bool
     */
    public function close()
    {
        try {
            if ($this->connection !== null) {
                $this->connection->close();
            }
        } catch (\Exception $exception) {
            // pass
        } finally {
            $this->connection = null;
        }

        try {
            if ($this->channel !== null) {
                $this->channel->close();
            }
        } catch (\Exception $exception) {
            // pass
        } finally {
            $this->connection = null;
        }

        $this->closed = true;
    }

    /**
     * @return bool
     */
    public function isClosed()
    {
        return $this->closed === true;
    }

    /**
     * @param \EventBand\Transport\Amqp\Driver\MessagePublication $publication
     * @param string                                              $exchange
     * @param string                                              $routingKey
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
            $this->close();

            throw new DriverException('Basic publish error', $e);
        }
    }

    /**
     * @param string   $queue
     * @param callable $callback
     * @param int      $timeout
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
                    // Check if we got interruption from system call, ex. on signal
                    // On some php version (ex. php 5.5 error is null on interrupted system call)
                    if (
                        $error === null
                        || stripos($error['message'], 'interrupted system call') !== false
                        || $error['type'] !== E_ERROR || $error['type'] !== E_WARNING // Don't consider E_ERROR and E_WARNING errors
                    ) {
                        break;
                    }

                    $this->close();

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

            $this->channel->close();
            $this->channel = null;
        } catch (\Exception $e) {
            $this->close();

            throw new DriverException('Basic consume error', $e);
        }
    }

    /**
     * @param \EventBand\Transport\Amqp\Driver\MessageDelivery $delivery
     */
    public function ack(MessageDelivery $delivery)
    {
        try {
            $this->getChannel()->basic_ack($delivery->getTag());
        } catch (\Exception $e) {
            $this->close();

            throw new DriverException('Basic ack error', $e);
        }
    }

    /**
     * @param \EventBand\Transport\Amqp\Driver\MessageDelivery $delivery
     */
    public function reject(MessageDelivery $delivery)
    {
        try {
            $this->getChannel()->basic_reject($delivery->getTag(), true);
        } catch (\Exception $e) {
            $this->close();

            throw new DriverException('Basic reject error', $e);
        }
    }

    /**
     * @param \EventBand\Transport\Amqp\Definition\ExchangeDefinition $exchange
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
            $this->close();

            throw new DriverException('Exchange declare error', $e);
        }
    }

    /**
     * @param string $target
     * @param string $source
     * @param string $routingKey
     */
    public function bindExchange($target, $source, $routingKey = '')
    {
        try {
            $this->getChannel()->exchange_bind($target, $source, $routingKey);
        } catch (\Exception $e) {
            $this->close();

            throw new DriverException(sprintf('Exchange bind error "%s":"%s"->"%s"', $source, $routingKey, $target), $e);
        }
    }

    /**
     * @param \EventBand\Transport\Amqp\Definition\QueueDefinition $queue
     */
    public function declareQueue(QueueDefinition $queue)
    {
        try {
            $this->getChannel()->queue_declare(
                $queue->getName(),
                false,
                $queue->isDurable(),
                $queue->isExclusive(),
                $queue->isAutoDeleted(),
                false,
                $queue->getArguments()
            );
        } catch (\Exception $e) {
            $this->close();

            throw new DriverException('Queue declare error', $e);
        }
    }

    /**
     * @param string $queue
     * @param string $exchange
     * @param string $routingKey
     */
    public function bindQueue($queue, $exchange, $routingKey = '')
    {
        try {
            $this->getChannel()->queue_bind($queue, $exchange, $routingKey);
        } catch (\Exception $e) {
            $this->close();

            throw new DriverException(sprintf('Exchange bind error "%s":"%s"->"%s"', $exchange, $routingKey, $queue), $e);
        }
    }

    /**
     * @return AMQPSocketConnection
     */
    protected function getConnection()
    {
        if (!$this->connection) {
            if (!$this->definition) {
                throw new \BadMethodCallException('Neither connection nor definition was set');
            }

            try {
                $this->connection = new AMQPSocketConnection(
                    $this->definition->getHost(),
                    $this->definition->getPort(),
                    $this->definition->getUser(),
                    $this->definition->getPassword(),
                    $this->definition->getVirtualHost()
                );
            } catch (\Exception $exception) {
                throw new DriverException('Can\'t connection to RabbitMQ node', $exception);
            }
        }

        return $this->connection;
    }

    /**
     * Get channel
     *
     * @return \PhpAmqpLib\Channel\AMQPChannel
     */
    protected function getChannel()
    {
        if (!$this->channel) {
            $this->channel = $this->getConnection()->channel();
        }

        return $this->channel;
    }
}
