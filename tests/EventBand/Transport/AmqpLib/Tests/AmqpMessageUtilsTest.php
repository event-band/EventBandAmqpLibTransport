<?php
/**
 * @LICENSE_TEXT
 */

namespace EventBand\Transport\AmqpLib\Tests;

use EventBand\Transport\Amqp\Driver\AmqpMessage;
use EventBand\Transport\AmqpLib\AmqpMessageUtils;
use PhpAmqpLib\Message\AMQPMessage as AmqpLibMessage;
use PHPUnit_Framework_TestCase as TestCase;

/**
 * Class AmqpMessageUtilsTest
 *
 * @author Kirill chEbba Chebunin <iam@chebba.org>
 */
class AmqpMessageUtilsTest extends TestCase
{
    /**
     * @test createMessage copy properties from amqp lib message to driver one
     */
    public function copyPropertiesFromAmqpLib()
    {
        $properties = [
            'application_headers' => [
                'x-header-1' => 'value1',
                'x-header-2' => 'value2'
            ],
            'content_type' => 'application/json',
            'content_encoding' => 'utf-8',
            'message_id' => '100',
            'app_id' => '2',
            'user_id' => '10',
            'priority' => 3,
            'timestamp' => strtotime('2012-12-18 09:45:11'),
            'expiration' => 1000,
            'type' => 'type_str',
            'reply_to' => 'foo.bar'
        ];

        $amqpLibMessage = new AmqpLibMessage('body content', $properties);
        $message = AmqpMessageUtils::createMessage($amqpLibMessage);

        $this->assertEquals('body content', $message->getBody());
        $this->assertEquals($properties['application_headers'], $message->getHeaders());
        $this->assertEquals($properties['content_type'], $message->getContentType());
        $this->assertEquals($properties['content_encoding'], $message->getContentEncoding());
        $this->assertEquals($properties['message_id'], $message->getMessageId());
        $this->assertEquals($properties['app_id'], $message->getAppId());
        $this->assertEquals($properties['user_id'], $message->getUserId());
        $this->assertEquals($properties['priority'], $message->getPriority());
        $this->assertEquals($properties['timestamp'], $message->getTimestamp());
        $this->assertEquals($properties['expiration'], $message->getExpiration());
        $this->assertEquals($properties['type'], $message->getType());
        $this->assertEquals($properties['reply_to'], $message->getReplyTo());
    }

    /**
     * @test createAmqpLibMessage copy properties from driver message to amqp lib message one
     */
    public function copyPropertiesToAmqpLib()
    {
        $properties = [
            'body' => 'body content',
            'headers' => [
                'x-header-1' => 'value1',
                'x-header-2' => 'value2'
            ],
            'contentType' => 'application/json',
            'contentEncoding' => 'utf-8',
            'messageId' => '100',
            'appId' => '2',
            'userId' => '10',
            'priority' => 3,
            'timestamp' => strtotime('2012-12-18 09:45:11'),
            'expiration' => 1000,
            'type' => 'type_str',
            'replyTo' => 'foo.bar'
        ];

        $message = $this->createMessage($properties);

        $amqpLibMessage = AmqpMessageUtils::createAmqpLibMessage($message);

        $this->assertEquals('body content', $amqpLibMessage->body);
        $this->assertEquals($properties['headers'], $amqpLibMessage->get('application_headers'));
        $this->assertEquals($properties['contentType'], $amqpLibMessage->get('content_type'));
        $this->assertEquals($properties['contentEncoding'], $amqpLibMessage->get('content_encoding'));
        $this->assertEquals($properties['messageId'], $amqpLibMessage->get('message_id'));
        $this->assertEquals($properties['appId'], $amqpLibMessage->get('app_id'));
        $this->assertEquals($properties['userId'], $amqpLibMessage->get('user_id'));
        $this->assertEquals($properties['priority'], $amqpLibMessage->get('priority'));
        $this->assertEquals($properties['timestamp'], $amqpLibMessage->get('timestamp'));
        $this->assertEquals($properties['expiration'], $amqpLibMessage->get('expiration'));
        $this->assertEquals($properties['type'], $amqpLibMessage->get('type'));
        $this->assertEquals($properties['replyTo'], $amqpLibMessage->get('reply_to'));
    }

    /**
     * @test createDelivery create delivery from amqp lib message delivery_info
     */
    public function deliveryInfo()
    {
        $amqpLibMessage = new AmqpLibMessage('body content');
        $amqpLibMessage->delivery_info = [
            'delivery_tag' => '12345',
            'exchange' => 'foo.bar',
            'routing_key' => 'foo.#',
            'redelivered' => true,
        ];

        $delivery = AmqpMessageUtils::createDelivery($amqpLibMessage, 'foo');

        $this->assertEquals('foo', $delivery->getQueue());
        $this->assertEquals('body content', $delivery->getMessage()->getBody());
        $this->assertEquals('12345', $delivery->getTag());
        $this->assertEquals('foo.bar', $delivery->getExchange());
        $this->assertEquals('foo.#', $delivery->getRoutingKey());
        $this->assertTrue($delivery->isRedelivered());
    }

    /**
     * @param array $properties
     * @return \PHPUnit_Framework_MockObject_MockObject|AmqpMessage
     */
    private function createMessage(array $properties)
    {
        $message = $this->getMock('EventBand\\Transport\\Amqp\\Driver\\AmqpMessage');
        foreach ($properties as $key => $value) {
            $message
                ->expects($this->any())
                ->method('get'.ucfirst($key))
                ->will($this->returnValue($value))
            ;
        }

        return $message;
    }
}
