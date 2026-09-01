<?php

declare(strict_types=1);

namespace PetitPress\GpsMessengerBundle\Tests\Transport;

use Google\Cloud\PubSub\BatchPublisher;
use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Topic;
use PetitPress\GpsMessengerBundle\Transport\GpsBatchSender;
use PetitPress\GpsMessengerBundle\Transport\GpsConfigurationInterface;
use PetitPress\GpsMessengerBundle\Transport\Stamp\AttributesStamp;
use PetitPress\GpsMessengerBundle\Transport\Stamp\OrderingKeyStamp;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

/**
 * @author Ronald Marfoldi <ronald.marfoldi@petitpress.sk>
 */
class GpsBatchSenderTest extends TestCase
{
    private const ORDERED_KEY = 'ordered-key';
    private const TOPIC_NAME = 'topic-name';

    /**
     * @var GpsConfigurationInterface&MockObject
     */
    private MockObject $gpsConfigurationMock;

    /**
     * @var PubSubClient&MockObject
     */
    private MockObject $pubSubClientMock;

    /**
     * @var SerializerInterface&MockObject
     */
    private MockObject $serializerMock;

    /**
     * @var Topic&MockObject
     */
    private MockObject $topicMock;

    /**
     * @var BatchPublisher&MockObject
     */
    private MockObject $batchPublisherMock;

    private GpsBatchSender $gpsBatchSender;

    protected function setUp(): void
    {
        $this->gpsConfigurationMock = $this->createMock(GpsConfigurationInterface::class);
        $this->pubSubClientMock = $this->createMock(PubSubClient::class);
        $this->serializerMock = $this->createMock(SerializerInterface::class);
        $this->topicMock = $this->createMock(Topic::class);
        $this->batchPublisherMock = $this->createMock(BatchPublisher::class);

        $this->gpsConfigurationMock
            ->method('shouldCompressMessageBody')
            ->willReturn(false)
        ;

        $this->gpsConfigurationMock
            ->method('getBatchSenderOptions')
            ->willReturn(['enabled' => true])
        ;

        $this->gpsBatchSender = new GpsBatchSender(
            $this->pubSubClientMock,
            $this->gpsConfigurationMock,
            $this->serializerMock,
        );
    }

    public function testItDoesNotPublishIfTheLastStampIsOfTypeRedeliveryWithRedeliveryDisabled(): void
    {
        $envelope = EnvelopeFactory::create(new RedeliveryStamp(0));
        $envelopeArray = ['body' => []];

        $this->serializerMock
            ->expects(static::once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($envelopeArray)
        ;

        $this->pubSubClientMock
            ->expects(static::never())
            ->method('topic')
        ;

        $this->gpsConfigurationMock
            ->expects(static::once())
            ->method('shouldUseMessengerRetry')
            ->willReturn(false);

        self::assertSame($envelope, $this->gpsBatchSender->send($envelope));
    }

    public function testItPublishesWithOrderingKey(): void
    {
        $envelope = EnvelopeFactory::create(new OrderingKeyStamp(self::ORDERED_KEY));
        $envelopeArray = ['body' => []];

        $this->serializerMock
            ->expects(static::once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($envelopeArray)
        ;

        $this->gpsConfigurationMock
            ->expects(static::once())
            ->method('getTopicName')
            ->willReturn(self::TOPIC_NAME)
        ;

        $this->pubSubClientMock
            ->expects(static::once())
            ->method('topic')
            ->with(self::TOPIC_NAME)
            ->willReturn($this->topicMock);

        $this->topicMock
            ->expects(static::once())
            ->method('batchPublisher')
            ->willReturn($this->batchPublisherMock)
        ;

        $this->batchPublisherMock
            ->expects(static::once())
            ->method('publish')
            ->with(new Message(['data' => json_encode($envelopeArray), 'orderingKey' => self::ORDERED_KEY]))
        ;

        self::assertSame($envelope, $this->gpsBatchSender->send($envelope));
    }

    public function testItPublishesWithAttributes(): void
    {
        $attributes = ['foo' => 'bar'];
        $envelope = EnvelopeFactory::create(new AttributesStamp($attributes));
        $envelopeArray = ['body' => []];

        $this->serializerMock
            ->expects(static::once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($envelopeArray)
        ;

        $this->gpsConfigurationMock
            ->expects(static::once())
            ->method('getTopicName')
            ->willReturn(self::TOPIC_NAME)
        ;

        $this->pubSubClientMock
            ->expects(static::once())
            ->method('topic')
            ->with(self::TOPIC_NAME)
            ->willReturn($this->topicMock);

        $this->topicMock
            ->expects(static::once())
            ->method('batchPublisher')
            ->willReturn($this->batchPublisherMock)
        ;

        $this->batchPublisherMock
            ->expects(static::once())
            ->method('publish')
            ->with(new Message([
                'data' => json_encode($envelopeArray),
                'attributes' => $attributes,
            ]))
        ;

        self::assertSame($envelope, $this->gpsBatchSender->send($envelope));
    }

    public function testItUsesCustomBatchOptions(): void
    {
        $envelope = EnvelopeFactory::create();
        $envelopeArray = ['body' => []];

        $this->gpsConfigurationMock = $this->createMock(GpsConfigurationInterface::class);
        $this->gpsConfigurationMock
            ->method('shouldCompressMessageBody')
            ->willReturn(false)
        ;
        $this->gpsConfigurationMock
            ->method('getBatchSenderOptions')
            ->willReturn(['enabled' => true, 'batchSize' => 50, 'callPeriod' => 0.5])
        ;

        $this->serializerMock
            ->expects(static::once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($envelopeArray)
        ;

        $this->gpsConfigurationMock
            ->expects(static::once())
            ->method('getTopicName')
            ->willReturn(self::TOPIC_NAME)
        ;

        $this->pubSubClientMock
            ->expects(static::once())
            ->method('topic')
            ->with(self::TOPIC_NAME)
            ->willReturn($this->topicMock);

        $this->topicMock
            ->expects(static::once())
            ->method('batchPublisher')
            ->with(['batchSize' => 50, 'callPeriod' => 0.5])
            ->willReturn($this->batchPublisherMock)
        ;

        $this->batchPublisherMock
            ->expects(static::once())
            ->method('publish')
        ;

        $gpsBatchSender = new GpsBatchSender(
            $this->pubSubClientMock,
            $this->gpsConfigurationMock,
            $this->serializerMock,
        );

        self::assertSame($envelope, $gpsBatchSender->send($envelope));
    }

    public function testItCompressesMessageBodyWhenEnabled(): void
    {
        $envelope = EnvelopeFactory::create();
        $envelopeArray = ['body' => 'hello'];

        $this->serializerMock
            ->expects(static::once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($envelopeArray)
        ;

        $this->gpsConfigurationMock = $this->createMock(GpsConfigurationInterface::class);
        $this->gpsConfigurationMock
            ->method('shouldCompressMessageBody')
            ->willReturn(true)
        ;
        $this->gpsConfigurationMock
            ->method('getBatchSenderOptions')
            ->willReturn(['enabled' => true])
        ;

        $this->gpsConfigurationMock
            ->expects(static::once())
            ->method('getTopicName')
            ->willReturn(self::TOPIC_NAME)
        ;

        $expectedData = gzencode((string) json_encode($envelopeArray));

        $this->pubSubClientMock
            ->expects(static::once())
            ->method('topic')
            ->with(self::TOPIC_NAME)
            ->willReturn($this->topicMock);

        $this->topicMock
            ->expects(static::once())
            ->method('batchPublisher')
            ->willReturn($this->batchPublisherMock)
        ;

        $this->batchPublisherMock
            ->expects(static::once())
            ->method('publish')
            ->with(new Message([
                'data' => $expectedData,
                'attributes' => ['compressed-message-body' => 'true'],
            ]))
        ;

        $gpsBatchSender = new GpsBatchSender(
            $this->pubSubClientMock,
            $this->gpsConfigurationMock,
            $this->serializerMock,
        );

        self::assertSame($envelope, $gpsBatchSender->send($envelope));
    }

    public function testItPublishesHeadersAsAttributesWhenEnabled(): void
    {
        $envelope = EnvelopeFactory::create();
        $envelopeArray = ['body' => 'hello', 'headers' => ['type' => 'stdClass']];

        $this->serializerMock
            ->expects(static::once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($envelopeArray)
        ;

        $this->gpsConfigurationMock = $this->createMock(GpsConfigurationInterface::class);
        $this->gpsConfigurationMock
            ->method('shouldCompressMessageBody')
            ->willReturn(false)
        ;
        $this->gpsConfigurationMock
            ->method('getBatchSenderOptions')
            ->willReturn(['enabled' => true])
        ;
        $this->gpsConfigurationMock
            ->method('shouldUseHeadersAsAttributes')
            ->willReturn(true)
        ;

        $this->gpsConfigurationMock
            ->expects(static::once())
            ->method('getTopicName')
            ->willReturn(self::TOPIC_NAME)
        ;

        $this->pubSubClientMock
            ->expects(static::once())
            ->method('topic')
            ->with(self::TOPIC_NAME)
            ->willReturn($this->topicMock);

        $this->topicMock
            ->expects(static::once())
            ->method('batchPublisher')
            ->willReturn($this->batchPublisherMock)
        ;

        $this->batchPublisherMock
            ->expects(static::once())
            ->method('publish')
            ->with(new Message([
                'data' => 'hello',
                'attributes' => ['type' => 'stdClass'],
            ]))
        ;

        $gpsBatchSender = new GpsBatchSender(
            $this->pubSubClientMock,
            $this->gpsConfigurationMock,
            $this->serializerMock,
        );

        self::assertSame($envelope, $gpsBatchSender->send($envelope));
    }
}
