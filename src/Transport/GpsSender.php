<?php

declare(strict_types=1);

namespace PetitPress\GpsMessengerBundle\Transport;

use Google\Cloud\PubSub\MessageBuilder;
use Google\Cloud\PubSub\PubSubClient;
use PetitPress\GpsMessengerBundle\Transport\Stamp\AttributesStamp;
use PetitPress\GpsMessengerBundle\Transport\Stamp\GpsSenderOptionsStamp;
use PetitPress\GpsMessengerBundle\Transport\Stamp\OrderingKeyStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

/**
 * @author Ronald Marfoldi <ronald.marfoldi@petitpress.sk>
 */
final class GpsSender implements SenderInterface
{
    private PubSubClient $pubSubClient;
    private GpsConfigurationInterface $gpsConfiguration;
    private SerializerInterface $serializer;

    public function __construct(
        PubSubClient $pubSubClient,
        GpsConfigurationInterface $gpsConfiguration,
        SerializerInterface $serializer
    ) {
        $this->pubSubClient = $pubSubClient;
        $this->gpsConfiguration = $gpsConfiguration;
        $this->serializer = $serializer;
    }

    /**
     * {@inheritdoc}
     */
    public function send(Envelope $envelope): Envelope
    {
        $encodedMessage = $this->serializer->encode($envelope);

        $messageBuilder = new MessageBuilder();
        $headersAsAttributes = $this->gpsConfiguration->shouldUseHeadersAsAttributes();
        if ($headersAsAttributes) {
            $data = $encodedMessage['body'];
        } else {
            try {
                $data = json_encode($encodedMessage, JSON_THROW_ON_ERROR);
            } catch (\JsonException $exception) {
                throw new TransportException($exception->getMessage(), 0, $exception);
            }
        }

        $compressMessageBody = $this->gpsConfiguration->shouldCompressMessageBody();
        if ($compressMessageBody) {
            if (! \function_exists('gzencode')) {
                throw new TransportException('Message body compression requires the "zlib" PHP extension.');
            }

            $compressedData = gzencode($data);
            if (false === $compressedData) {
                throw new TransportException('Failed to compress message body.');
            }

            $data = $compressedData;
        }

        $messageBuilder = $messageBuilder->setData($data);
        if ($compressMessageBody) {
            $messageBuilder = $messageBuilder->addAttribute('compressed-message-body', 'true');
        }

        if ($headersAsAttributes) {
            foreach ($encodedMessage['headers'] ?? [] as $headerName => $headerValue) {
                $messageBuilder = $messageBuilder->addAttribute($headerName, $headerValue);
            }
        }

        if (! $this->gpsConfiguration->shouldUseMessengerRetry()) {
            $redeliveryStamp = $envelope->last(RedeliveryStamp::class);
            if ($redeliveryStamp instanceof RedeliveryStamp) {
                // do not try to redeliver, message wasn't acknowledged, so let's Google Pub/Sub do its job with retry policy
                return $envelope;
            }
        }

        $orderingKeyStamp = $envelope->last(OrderingKeyStamp::class);
        if ($orderingKeyStamp instanceof OrderingKeyStamp) {
            $messageBuilder = $messageBuilder->setOrderingKey($orderingKeyStamp->getOrderingKey());
        }

        $attributesStamp = $envelope->last(AttributesStamp::class);
        if ($attributesStamp instanceof AttributesStamp) {
            $messageBuilder = $messageBuilder->setAttributes($attributesStamp->getAttributes());
        }

        $senderOptionsStamp = $envelope->last(GpsSenderOptionsStamp::class);
        $options = [];
        if ($senderOptionsStamp instanceof GpsSenderOptionsStamp) {
            $options = $senderOptionsStamp->getOptions();
        }
        $this->pubSubClient
            ->topic($this->gpsConfiguration->getTopicName())
            ->publish($messageBuilder->build(), $options)
        ;

        return $envelope;
    }
}
