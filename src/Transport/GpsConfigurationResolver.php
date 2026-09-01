<?php

declare(strict_types=1);

namespace PetitPress\GpsMessengerBundle\Transport;

use Symfony\Component\OptionsResolver\Options;
use Symfony\Component\OptionsResolver\OptionsResolver;

/**
 * @author Ronald Marfoldi <ronald.marfoldi@petitpress.sk>
 */
final class GpsConfigurationResolver implements GpsConfigurationResolverInterface
{
    private const INT_NORMALIZER_KEY = 'int';
    private const FLOAT_NORMALIZER_KEY = 'float';
    private const BOOL_NORMALIZER_KEY = 'bool';
    private const NORMALIZABLE_SUBSCRIPTION_OPTIONS = [
        self::INT_NORMALIZER_KEY => ['ackDeadlineSeconds', 'maxDeliveryAttempts'],
        self::BOOL_NORMALIZER_KEY => ['enableMessageOrdering', 'retainAckedMessages', 'enableExactlyOnceDelivery'],
    ];
    private const NORMALIZABLE_SUBSCRIPTION_PULL_OPTIONS = [
        self::INT_NORMALIZER_KEY => ['maxMessages', 'timeoutMillis'],
        self::BOOL_NORMALIZER_KEY => ['returnImmediately'],
    ];
    private const NORMALIZABLE_BATCH_SENDER_OPTIONS = [
        self::INT_NORMALIZER_KEY => ['batchSize'],
        self::FLOAT_NORMALIZER_KEY => ['callPeriod'],
        self::BOOL_NORMALIZER_KEY => ['enabled'],
    ];

    /**
     * {@inheritdoc}
     */
    public function resolve(string $dsn, array $options): GpsConfigurationInterface
    {
        // not relevant options for transport itself
        unset($options['transport_name'], $options['serializer']);

        $subscriptionOptionsNormalizer = static function (Options $options, $data) {
            return self::normalizeOptions(
                $data ?? [],
                self::NORMALIZABLE_SUBSCRIPTION_OPTIONS
            );
        };

        $subscriptionPullOptionsNormalizer = static function (Options $options, $data) {
            return self::normalizeOptions(
                $data ?? [],
                self::NORMALIZABLE_SUBSCRIPTION_PULL_OPTIONS
            );
        };

        $batchSenderOptionsNormalizer = static function (Options $options, $data) {
            return self::normalizeOptions(
                $data ?? [],
                self::NORMALIZABLE_BATCH_SENDER_OPTIONS
            );
        };

        $mergedOptions = $this->getMergedOptions($dsn, $options);

        $optionsResolver = new OptionsResolver();
        $optionsResolver
            ->setDefault('use_messenger_retry', false)
            ->setAllowedTypes('use_messenger_retry', 'bool')
            ->setDefault('compress_message_body', false)
            ->setAllowedTypes('compress_message_body', 'bool')
            ->setDefault('headers_as_attributes', false)
            ->setAllowedTypes('headers_as_attributes', 'bool')
            ->setDefault('batchSender', ['enabled' => false])
            ->setAllowedTypes('batchSender', 'array')
            ->setNormalizer('batchSender', $batchSenderOptionsNormalizer)
            ->setDefault('client_config', [])
            ->setOptions('topic', function (OptionsResolver $topicResolver): void {
                $topicResolver
                    ->setDefault('name', self::DEFAULT_TOPIC_NAME)
                    ->setDefault('createIfNotExist', true)
                    ->setDefault('options', [])
                    ->setAllowedTypes('name', 'string')
                    ->setAllowedTypes('createIfNotExist', 'bool')
                    ->setAllowedTypes('options', 'array')
                ;
            })
            ->setOptions(
                'subscription',
                function (OptionsResolver $resolver, Options $parentOptions) use ($subscriptionOptionsNormalizer, $subscriptionPullOptionsNormalizer): void {
                    $resolver
                        ->setDefault('name', $parentOptions['topic']['name'])
                        ->setDefault('createIfNotExist', true)
                        ->setDefault('options', [])
                        ->setOptions(
                            'pull',
                            function (OptionsResolver $pullResolver): void {
                                $pullResolver
                                    ->setDefault('maxMessages', self::DEFAULT_MAX_MESSAGES_PULL)
                                    ->setDefined('returnImmediately')
                                    ->setDefined('timeoutMillis')
                                ;
                            }
                        )
                        ->setAllowedTypes('name', 'string')
                        ->setAllowedTypes('createIfNotExist', 'bool')
                        ->setAllowedTypes('options', 'array')
                        ->setAllowedTypes('pull', 'array')
                        ->setNormalizer('options', $subscriptionOptionsNormalizer)
                        ->setNormalizer('pull', $subscriptionPullOptionsNormalizer)
                    ;
                }
            )
            ->setAllowedTypes('client_config', 'array')
        ;

        $resolvedOptions = $optionsResolver->resolve($mergedOptions);

        return new GpsConfiguration(
            $resolvedOptions['topic']['name'],
            $resolvedOptions['topic']['createIfNotExist'],
            $resolvedOptions['subscription']['name'],
            $resolvedOptions['subscription']['createIfNotExist'],
            $resolvedOptions['use_messenger_retry'],
            $resolvedOptions['compress_message_body'],
            $resolvedOptions['client_config'],
            $resolvedOptions['topic']['options'],
            $resolvedOptions['subscription']['options'],
            $resolvedOptions['subscription']['pull'],
            $resolvedOptions['batchSender'],
            $resolvedOptions['headers_as_attributes']
        );
    }

    /**
     * @param array<mixed, mixed> $data
     * @param array<string, array<array-key, string>> $normalizationRules
     *
     * @return array<string, mixed>
     */
    private static function normalizeOptions(array $data, array $normalizationRules): array
    {
        foreach ($data as $optionName => &$optionValue) {
            if (\is_array($optionValue)) {
                // normalize nested arrays
                $optionValue = self::normalizeOptions($optionValue, $normalizationRules);
                continue;
            }

            switch (true) {
                case \in_array($optionName, $normalizationRules[self::INT_NORMALIZER_KEY], true):
                    $optionValue = (int) filter_var($optionValue, FILTER_SANITIZE_NUMBER_INT);
                    break;
                case \in_array($optionName, $normalizationRules[self::FLOAT_NORMALIZER_KEY] ?? [], true):
                    $optionValue = (float) filter_var($optionValue, FILTER_SANITIZE_NUMBER_FLOAT, FILTER_FLAG_ALLOW_FRACTION);
                    break;
                case \in_array($optionName, $normalizationRules[self::BOOL_NORMALIZER_KEY], true):
                    $optionValue = filter_var($optionValue, FILTER_VALIDATE_BOOLEAN);
                    break;
            }
        }

        return $data;
    }

    /**
     * @param array<string, mixed>  $options
     *
     * @return array<int|string, mixed>
     */
    private function getMergedOptions(string $dsn, array $options): array
    {
        $dnsOptions = [];
        $parsedDnsOptions = parse_url($dsn);

        $dsnQueryOptions = $parsedDnsOptions['query'] ?? null;
        if ($dsnQueryOptions !== null && $dsnQueryOptions !== '') {
            parse_str($dsnQueryOptions, $dnsOptions);
        }

        $dnsPathOption = $parsedDnsOptions['path'] ?? null;
        if ($dnsPathOption !== null && $dnsPathOption !== '') {
            if (! isset($dnsOptions['topic']) || ! is_array($dnsOptions['topic'])) {
                $dnsOptions['topic'] = [];
            }
            $dnsOptions['topic']['name'] = substr($dnsPathOption, 1);
        }

        if (isset($dnsOptions['use_messenger_retry']) && is_string($dnsOptions['use_messenger_retry'])) {
            $dnsOptions['use_messenger_retry'] = $this->toBool($dnsOptions['use_messenger_retry'], false);
        }

        if (isset($dnsOptions['headers_as_attributes']) && is_string($dnsOptions['headers_as_attributes'])) {
            $dnsOptions['headers_as_attributes'] = $this->toBool($dnsOptions['headers_as_attributes'], false);
        }

        if (isset($dnsOptions['topic']['createIfNotExist']) && is_string($dnsOptions['topic']['createIfNotExist'])) {
            $dnsOptions['topic']['createIfNotExist'] = $this->toBool($dnsOptions['topic']['createIfNotExist'], true);
        }

        if (isset($dnsOptions['subscription']['createIfNotExist']) && is_string($dnsOptions['subscription']['createIfNotExist'])) {
            $dnsOptions['subscription']['createIfNotExist'] = $this->toBool($dnsOptions['subscription']['createIfNotExist'], true);
        }

        return array_merge($dnsOptions, $options);
    }

    private function toBool(string $value, bool $default): bool
    {
        $result = filter_var($value, FILTER_VALIDATE_BOOLEAN, FILTER_NULL_ON_FAILURE);

        return $result ?? $default;
    }
}
