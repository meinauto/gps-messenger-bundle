<?php

declare(strict_types=1);

namespace PetitPress\GpsMessengerBundle\Tests\Transport;

use Exception;
use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use PetitPress\GpsMessengerBundle\Transport\GpsConfigurationInterface;
use PetitPress\GpsMessengerBundle\Transport\GpsReceiver;
use PetitPress\GpsMessengerBundle\Transport\Stamp\GpsReceivedStamp;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

/**
 * @author Mickael Prévôt <mickael.prevot@ext.adeo.com>
 * @author Ronald Marfoldi <ronald.marfoldi@petitpress.sk>
 */
class GpsReceiverTest extends TestCase
{
    private const SUBSCRIPTION_NAME = 'subscription-name';

    /**
     * @var GpsConfigurationInterface&MockObject
     */
    private MockObject $gpsConfigurationMock;

    /**
     * @var PubSubClient&MockObject
     */
    private MockObject $pubSubClientMock;

    /**
     * @var Subscription&MockObject
     */
    private MockObject $subscriptionMock;

    /**
     * @var LoggerInterface&MockObject
     */
    private MockObject $loggerMock;

    private GpsReceiver $gpsReceiver;

    protected function setUp(): void
    {
        $this->gpsConfigurationMock = $this->createMock(GpsConfigurationInterface::class);
        $this->pubSubClientMock = $this->createMock(PubSubClient::class);
        $this->subscriptionMock = $this->createMock(Subscription::class);
        $this->loggerMock = $this->createMock(LoggerInterface::class);
        $serializerStub = static::createStub(SerializerInterface::class);

        $this->gpsReceiver = new GpsReceiver(
            $this->pubSubClientMock,
            $this->gpsConfigurationMock,
            $serializerStub,
            $this->loggerMock,
        );
    }

    public function testItAcks(): void
    {
        $gpsMessage = new Message(['data' => '']);

        $this->gpsConfigurationMock
            ->expects(static::once())
            ->method('getSubscriptionName')
            ->willReturn(self::SUBSCRIPTION_NAME)
        ;

        $this->subscriptionMock
            ->expects(static::once())
            ->method('acknowledge')
            ->with($gpsMessage)
        ;

        $this->pubSubClientMock
            ->expects(static::once())
            ->method('subscription')
            ->with(self::SUBSCRIPTION_NAME)
            ->willReturn($this->subscriptionMock)
        ;

        $this->gpsReceiver->ack(
            EnvelopeFactory::create(new GpsReceivedStamp($gpsMessage))
        );
    }

    public function testItRejects(): void
    {
        $gpsMessage = new Message(['data' => '']);

        $this->gpsConfigurationMock
            ->expects(static::once())
            ->method('getSubscriptionName')
            ->willReturn(self::SUBSCRIPTION_NAME)
        ;

        $this->subscriptionMock
            ->expects(static::once())
            ->method('modifyAckDeadline')
            ->with($gpsMessage, 0)
        ;

        $this->pubSubClientMock
            ->expects(static::once())
            ->method('subscription')
            ->with(self::SUBSCRIPTION_NAME)
            ->willReturn($this->subscriptionMock)
        ;

        $this->gpsReceiver->reject(
            EnvelopeFactory::create(new GpsReceivedStamp($gpsMessage))
        );
    }


    #[DataProvider('keepAliveSeconds')]
    public function testItKeepalive(?int $actualSeconds, int $expectedSeconds): void
    {
        $gpsMessage = new Message(['data' => '']);

        $this->gpsConfigurationMock
            ->expects(static::once())
            ->method('getSubscriptionName')
            ->willReturn(self::SUBSCRIPTION_NAME)
        ;

        $this->subscriptionMock
            ->expects(static::once())
            ->method('modifyAckDeadline')
            ->with($gpsMessage, $expectedSeconds)
        ;

        $this->pubSubClientMock
            ->expects(static::once())
            ->method('subscription')
            ->with(self::SUBSCRIPTION_NAME)
            ->willReturn($this->subscriptionMock)
        ;

        $this->gpsReceiver->keepalive(
            EnvelopeFactory::create(new GpsReceivedStamp($gpsMessage)),
            $actualSeconds
        );
    }

    public function testItThrowsAnExceptionOnErrorWhenKeepingAlive(): void
    {
        $gpsMessage = new Message(['data' => '']);

        $this->gpsConfigurationMock
            ->expects(static::once())
            ->method('getSubscriptionName')
            ->willReturn(self::SUBSCRIPTION_NAME)
        ;

        $this->subscriptionMock
            ->expects(static::once())
            ->method('modifyAckDeadline')
            ->with($gpsMessage)
            ->willThrowException(new Exception('Some error with modifying ack deadline.'))
        ;

        $this->pubSubClientMock
            ->expects(static::once())
            ->method('subscription')
            ->with(self::SUBSCRIPTION_NAME)
            ->willReturn($this->subscriptionMock)
        ;

        $this->expectException(TransportException::class);
        $this->expectExceptionCode(0);
        $this->expectExceptionMessage('Some error with modifying ack deadline.');

        $this->gpsReceiver->keepalive(
            EnvelopeFactory::create(new GpsReceivedStamp($gpsMessage)),
        );
    }

    public function testItLogsAndAcknowledgesUndecodableMessages(): void
    {
        $gpsMessage = new Message(['data' => 'not-valid-json']);

        $this->gpsConfigurationMock
            ->expects(static::exactly(2))
            ->method('getSubscriptionName')
            ->willReturn(self::SUBSCRIPTION_NAME)
        ;

        $this->gpsConfigurationMock
            ->expects(static::once())
            ->method('getSubscriptionPullOptions')
            ->willReturn([])
        ;

        $this->subscriptionMock
            ->expects(static::once())
            ->method('pull')
            ->willReturn([$gpsMessage])
        ;

        $this->subscriptionMock
            ->expects(static::once())
            ->method('acknowledge')
            ->with($gpsMessage)
        ;

        $this->pubSubClientMock
            ->expects(static::exactly(2))
            ->method('subscription')
            ->with(self::SUBSCRIPTION_NAME)
            ->willReturn($this->subscriptionMock)
        ;

        $this->loggerMock
            ->expects(static::once())
            ->method('warning')
        ;

        $envelopes = iterator_to_array($this->gpsReceiver->get());

        static::assertSame([], $envelopes);
    }

    public function testItDecompressesCompressedMessageBody(): void
    {
        $decodedBody = ['body' => 'hello', 'headers' => []];
        $compressedData = gzencode((string) json_encode($decodedBody));
        $gpsMessage = new Message([
            'data' => $compressedData,
            'attributes' => ['compressed-message-body' => 'true'],
        ]);

        $this->gpsConfigurationMock
            ->expects(static::once())
            ->method('getSubscriptionName')
            ->willReturn(self::SUBSCRIPTION_NAME)
        ;

        $this->gpsConfigurationMock
            ->expects(static::once())
            ->method('getSubscriptionPullOptions')
            ->willReturn([])
        ;

        $this->subscriptionMock
            ->expects(static::once())
            ->method('pull')
            ->willReturn([$gpsMessage])
        ;

        $this->pubSubClientMock
            ->expects(static::once())
            ->method('subscription')
            ->with(self::SUBSCRIPTION_NAME)
            ->willReturn($this->subscriptionMock)
        ;

        $serializerMock = $this->createMock(SerializerInterface::class);
        $serializerMock
            ->expects(static::once())
            ->method('decode')
            ->with($decodedBody)
            ->willReturn(EnvelopeFactory::create())
        ;

        $gpsReceiver = new GpsReceiver(
            $this->pubSubClientMock,
            $this->gpsConfigurationMock,
            $serializerMock,
            $this->loggerMock,
        );

        $envelopes = iterator_to_array($gpsReceiver->get());

        static::assertCount(1, $envelopes);
    }

    #[AllowMockObjectsWithoutExpectations]
    public function testItThrowsAnExceptionInsteadOfRejecting(): void
    {
        $this->expectException(TransportException::class);
        $this->expectExceptionCode(0);
        $this->expectExceptionMessage('No GpsReceivedStamp found on the Envelope.');

        $this->gpsReceiver->reject(EnvelopeFactory::create());
    }

    /**
     * @return array<int, array<int, int|null>>
     */
    public static function keepAliveSeconds(): array
    {
        return [
            [null, 5],
            [15, 15]
        ];
    }
}
