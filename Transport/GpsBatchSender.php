<?php

declare(strict_types=1);

namespace PetitPress\GpsMessengerBundle\Transport;

use Google\Cloud\PubSub\BatchPublisher;
use Google\Cloud\PubSub\MessageBuilder;
use Google\Cloud\PubSub\PubSubClient;
use PetitPress\GpsMessengerBundle\Transport\Stamp\AttributesStamp;
use PetitPress\GpsMessengerBundle\Transport\Stamp\OrderingKeyStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

final class GpsBatchSender implements SenderInterface
{
    private PubSubClient $pubSubClient;
    private GpsConfigurationInterface $gpsConfiguration;
    private SerializerInterface $serializer;
    private ?BatchPublisher $batchPublisher = null;
    private array $batchOptions = [
        'batchSize' => 100,  // Max messages for each batch.
        'callPeriod' => 0.1, // Max time in seconds between each batch publish.
    ];

    public function __construct(
        PubSubClient $pubSubClient,
        GpsConfigurationInterface $gpsConfiguration,
        SerializerInterface $serializer
    ) {
        $this->pubSubClient = $pubSubClient;
        $this->gpsConfiguration = $gpsConfiguration;
        $this->serializer = $serializer;
    }

    public function send(Envelope $envelope): Envelope
    {
        $encodedMessage = $this->serializer->encode($envelope);

        $messageBuilder = new MessageBuilder();
        $messageBuilder = $messageBuilder
            ->setData($encodedMessage['body'])
            ->setAttributes($encodedMessage['headers'] ?? []);

        $redeliveryStamp = $envelope->last(RedeliveryStamp::class);
        if ($redeliveryStamp instanceof RedeliveryStamp) {
            // do not try to redeliver, message wasn't acknowledged, so let's Google Pub/Sub do its job with retry policy
            return $envelope;
        }

        $orderingKeyStamp = $envelope->last(OrderingKeyStamp::class);
        if ($orderingKeyStamp instanceof OrderingKeyStamp) {
            $messageBuilder = $messageBuilder->setOrderingKey($orderingKeyStamp->getOrderingKey());
        }

        $attributesStamp = $envelope->last(AttributesStamp::class);
        if ($attributesStamp instanceof AttributesStamp) {
            foreach ($attributesStamp->getAttributes() as $key => $value) {
                $messageBuilder = $messageBuilder->addAttribute($key, $value);
            }
        }

        $this->getBatchPublisher()
            ->publish($messageBuilder->build());

        return $envelope;
    }

    private function getBatchPublisher(): BatchPublisher
    {
        if (null === $this->batchPublisher) {
            $this->batchPublisher = $this->pubSubClient
                ->topic($this->gpsConfiguration->getTopicName())
                ->batchPublisher($this->batchOptions);
        }

        return $this->batchPublisher;
    }
}
