<?php

declare(strict_types=1);

namespace PetitPress\GpsMessengerBundle\Transport;

/**
 * @author Ronald Marfoldi <ronald.marfoldi@petitpress.sk>
 */
final class GpsConfiguration implements GpsConfigurationInterface
{
    private string $topicName;
    private bool   $topicCreationEnabled;
    private string $subscriptionName;
    private bool   $subscriptionCreationEnabled;
    private bool   $useMessengerRetry;
    private bool   $compressMessageBody;

    /**
     * @var array<string, mixed>
     */
    private array  $clientConfig;

    /**
     * @var array<string, mixed>
     */
    private array  $topicOptions;

    /**
     * @var array<string, mixed>
     */
    private array  $subscriptionOptions;

    /**
     * @var array<string, mixed>
     */
    private array  $subscriptionPullOptions;

    /**
     * @var array<string, mixed>
     */
    private array  $batchSenderOptions;

    /**
     * @param array<string, mixed>  $clientConfig
     * @param array<string, mixed>  $topicOptions
     * @param array<string, mixed>  $subscriptionOptions
     * @param array<string, mixed>  $subscriptionPullOptions
     * @param array<string, mixed>  $batchSenderOptions
     */
    public function __construct(
        string $topicName,
        bool $topicCreationEnabled,
        string $subscriptionName,
        bool $subscriptionCreationEnabled,
        bool $useMessengerRetry,
        bool $compressMessageBody,
        array $clientConfig,
        array $topicOptions,
        array $subscriptionOptions,
        array $subscriptionPullOptions,
        array $batchSenderOptions = ['enabled' => false]
    ) {
        $this->topicName = $topicName;
        $this->topicCreationEnabled = $topicCreationEnabled;
        $this->subscriptionName = $subscriptionName;
        $this->subscriptionCreationEnabled = $subscriptionCreationEnabled;
        $this->useMessengerRetry = $useMessengerRetry;
        $this->compressMessageBody = $compressMessageBody;
        $this->clientConfig = $clientConfig;
        $this->topicOptions = $topicOptions;
        $this->subscriptionOptions = $subscriptionOptions;
        $this->subscriptionPullOptions = $subscriptionPullOptions;
        $this->batchSenderOptions = $batchSenderOptions;
    }

    public function getTopicName(): string
    {
        return $this->topicName;
    }

    public function isTopicCreationEnabled(): bool
    {
        return $this->topicCreationEnabled;
    }

    public function getSubscriptionName(): string
    {
        return $this->subscriptionName;
    }

    public function isSubscriptionCreationEnabled(): bool
    {
        return $this->subscriptionCreationEnabled;
    }

    public function shouldUseMessengerRetry(): bool
    {
        return $this->useMessengerRetry;
    }

    public function shouldCompressMessageBody(): bool
    {
        return $this->compressMessageBody;
    }

    public function getClientConfig(): array
    {
        return $this->clientConfig;
    }

    public function getTopicOptions(): array
    {
        return $this->topicOptions;
    }

    public function getSubscriptionOptions(): array
    {
        return $this->subscriptionOptions;
    }

    public function getSubscriptionPullOptions(): array
    {
        return $this->subscriptionPullOptions;
    }

    public function getBatchSenderOptions(): array
    {
        return $this->batchSenderOptions;
    }

    public function isBatchSenderEnabled(): bool
    {
        return (bool) ($this->batchSenderOptions['enabled'] ?? false);
    }
}
