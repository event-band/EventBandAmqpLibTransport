<?php
/**
 * @LICENSE_TEXT
 */

namespace EventBand\Transport\AmqpLib;

use PhpAmqpLib\Connection\AbstractConnection;

/**
 * Class AmqpConnectionFactory
 *
 * @author Kirill chEbba Chebunin <iam@chebba.org>
 */
interface AmqpConnectionFactory
{
    /**
     * @return AbstractConnection
     */
    public function getConnection();
}