<?php

declare(strict_types=1);

namespace PetitPress\GpsMessengerBundle\Transport;

use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use LogicException;
use PetitPress\GpsMessengerBundle\Transport\Stamp\GpsReceivedStamp;
use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Throwable;

/**
 * @author Ronald Marfoldi <ronald.marfoldi@petitpress.sk>
 */
final class GpsReceiver implements ReceiverInterface
{
    private PubSubClient $pubSubClient;

    private GpsConfigurationInterface $gpsConfiguration;

    private SerializerInterface $serializer;

    private LoggerInterface $logger;

    public function __construct(
        PubSubClient $pubSubClient,
        GpsConfigurationInterface $gpsConfiguration,
        SerializerInterface $serializer,
        LoggerInterface $logger
    ) {
        $this->pubSubClient = $pubSubClient;
        $this->gpsConfiguration = $gpsConfiguration;
        $this->serializer = $serializer;
        $this->logger = $logger;
    }

    /**
     * {@inheritdoc}
     *
     * @psalm-suppress InvalidReturnType
     */
    public function get(): iterable
    {
        try {
            $messages = $this->pubSubClient
                ->subscription($this->gpsConfiguration->getSubscriptionName())
                ->pull($this->gpsConfiguration->getSubscriptionPullOptions());

            foreach ($messages as $message) {
                try {
                    yield $this->createEnvelopeFromPubSubMessage($message);
                } catch (MessageDecodingFailedException $exception) {
                    $this->logger->warning($exception->getMessage(), ['exception' => $exception]);

                    $this->pubSubClient
                        ->subscription($this->gpsConfiguration->getSubscriptionName())
                        ->acknowledge($message);
                }
            }
        } catch (Throwable $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function ack(Envelope $envelope): void
    {
        try {
            $gpsReceivedStamp = $this->getGpsReceivedStamp($envelope);

            $this->pubSubClient
                ->subscription($this->gpsConfiguration->getSubscriptionName())
                ->acknowledge($gpsReceivedStamp->getGpsMessage());
        } catch (Throwable $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    /**
     * Called when handling the message failed and allows to warn PUB/SUB not to wait the ack.
     * After warning PUB/SUB, it will try to redeliver the message according to set up retry policy.
     *
     * @throws TransportException If there is an issue communicating with the transport
     *
     * @see https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions#RetryPolicy
     */
    public function reject(Envelope $envelope): void
    {
        $this->ack($envelope);
    }

    public function extendMessageTtl(Envelope $envelope, int $ttl): void
    {
        try {
            $gpsReceivedStamp = $this->getGpsReceivedStamp($envelope);

            $this->pubSubClient
                ->subscription($this->gpsConfiguration->getSubscriptionName())
                ->modifyAckDeadline($gpsReceivedStamp->getGpsMessage(), $ttl)
            ;
        } catch (Throwable $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    private function getGpsReceivedStamp(Envelope $envelope): GpsReceivedStamp
    {
        $gpsReceivedStamp = $envelope->last(GpsReceivedStamp::class);
        if ($gpsReceivedStamp instanceof GpsReceivedStamp) {
            return $gpsReceivedStamp;
        }

        throw new LogicException('No GpsReceivedStamp found on the Envelope.');
    }

    /**
     * Creates Symfony Envelope from Google Pub/Sub Message.
     * It adds stamp with received native Google Pub/Sub message.
     */
    private function createEnvelopeFromPubSubMessage(Message $message): Envelope
    {
        $headers = $message->attributes();

        $body = $message->data();
        if (isset($headers['compressed-message-body']) && $headers['compressed-message-body'] === "true") {
            $body = \gzdecode($body);
            if (false === $body) {
                throw new MessageDecodingFailedException('Failed to decode compressed message body.');
            }
        }

        $envelope = $this->serializer->decode([
            'body' => $body,
            'headers' => $headers,
        ]);

        return $envelope->with(new GpsReceivedStamp($message));
    }
}
