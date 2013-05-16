<?php
/**
 * @LICENSE_TEXT
 */

namespace EventBand\Transport\AmqpLib\Tests\Functional;

use EventBand\Transport\Amqp\Driver\Test\DriverFunctionalTestCase;
use EventBand\Transport\AmqpLib\AmqpLibDriver;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPSocketConnection;

/**
 * Class AmqpLibDriverTest
 *
 * @author Kirill chEbba Chebunin <iam@chebba.org>
 */
class AmqpLibDriverTest extends DriverFunctionalTestCase
{
    /**
     * @var AbstractConnection
     */
    private $conn;
    /**
     * @var AMQPChannel
     */
    private $channel;

    protected function createDriver()
    {
        $factory = $this->getMock('EventBand\Transport\AmqpLib\AmqpConnectionFactory');
        $factory
            ->expects($this->any())
            ->method('getConnection')
            ->will($this->returnValue($this->conn))
        ;
        return new AmqpLibDriver($factory);
    }

    protected function setUpAmqp()
    {
        $this->conn = new AMQPSocketConnection(
            $_SERVER['AMQP_HOST'],
            $_SERVER['AMQP_PORT'],
            $_SERVER['AMQP_USER'],
            $_SERVER['AMQP_PASS'],
            $_SERVER['AMQP_VHOST']
        );

        $this->channel = $this->conn->channel();

        $this->channel->exchange_declare('event_band.test.exchange', 'topic');
        $this->channel->queue_declare('event_band.test.event');
        $this->channel->queue_bind('event_band.test.event', 'event_band.test.exchange', 'event.#');
    }

    protected function tearDownAmqp()
    {
        $this->conn->close();
    }
}
