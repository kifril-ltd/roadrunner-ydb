package ydbjobs

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"go.uber.org/zap"
	"regexp"
	"sync"
	"time"
)

type ProducerFactory struct {
	client     topic.Client
	logger     *zap.Logger
	topicRegex *regexp.Regexp
	producerID string

	mu        sync.RWMutex
	producers map[string]Producer
}

func NewProducerFactory(client topic.Client, logger *zap.Logger, topicPattern string, producerID string) (*ProducerFactory, error) {
	regex, err := regexp.Compile(topicPattern)
	if err != nil {
		return nil, err
	}

	return &ProducerFactory{
		client:     client,
		logger:     logger,
		topicRegex: regex,
		producers:  make(map[string]Producer),
		producerID: producerID,
	}, nil
}

func (pf *ProducerFactory) Producer(ctx context.Context, topic string) (Producer, error) {
	if !pf.topicRegex.MatchString(topic) {
		return nil, fmt.Errorf("topic %s does not match pattern %s", topic, pf.topicRegex.String())
	}

	pf.mu.RLock()
	pr, ok := pf.producers[topic]
	pf.mu.RUnlock()
	if ok {
		return pr, nil
	}

	pf.mu.Lock()
	defer pf.mu.Unlock()

	pr, err := pf.buildProducer(ctx, topic)
	if err != nil {
		return nil, err
	}

	pf.producers[topic] = pr
	return pr, nil
}

func (pf *ProducerFactory) Stop(ctx context.Context) error {
	pf.mu.Lock()
	defer pf.mu.Unlock()

	for _, p := range pf.producers {
		if err := p.Stop(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (pf *ProducerFactory) buildProducer(ctx context.Context, topic string) (Producer, error) {
	writer, err := pf.client.StartWriter(topic,
		topicoptions.WithWriterCodec(topictypes.CodecRaw),
		topicoptions.WithWriterWaitServerAck(false),
		topicoptions.WithWriterProducerID(pf.producerID),
	)

	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := writer.WaitInit(ctx); err != nil {
		pf.logger.Error("Failed to wait for writer initialization", zap.Error(err))

		return nil, err
	}

	p := NewProducer(writer, pf.logger)

	return p, nil
}
