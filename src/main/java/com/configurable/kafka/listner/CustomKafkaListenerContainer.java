package com.configurable.kafka.listner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.configurable.kafka.listner.CustomKafkaListeners.CustomKafkaListener;

@Component
public class CustomKafkaListenerContainer implements SmartLifecycle {

	private static final Logger logger = LoggerFactory.getLogger(CustomKafkaListenerContainer.class);

	@Autowired
	ConsumerFactory<String, String> consumerFactory;

	private final Map<String, MessageListenerContainer> listenerContainers = new ConcurrentHashMap<String, MessageListenerContainer>();

	private static final String GENERATED_ID_PREFIX = "org.springframework.kafka.KafkaListenerEndpointContainer#";
	private final AtomicInteger counter = new AtomicInteger();

	@Autowired
	private CustomKafkaListeners customKafkaListeners;

	@Autowired
	private ObjectFactory<CustomMessageListener> consumerListenerFactory;

	@PostConstruct
	public void inIt() {

		for (CustomKafkaListener customKafkaListener : customKafkaListeners.getKafkaListeners()) {

			System.err.println(customKafkaListener.toString());
			this.registerListenerContainer(customKafkaListener);

		}

	}

	private void registerListenerContainer(CustomKafkaListener customKafkaListener) {
		Assert.notNull(customKafkaListener, "Endpoint must not be null");

		String id = getEndpointId(customKafkaListener);
		Assert.hasText(id, "Endpoint id must not be empty");

		synchronized (this.listenerContainers) {
			Assert.state(!this.listenerContainers.containsKey(id),
					"Another endpoint is already registered with id '" + id + "'");

			ConcurrentMessageListenerContainer<String, String> container = createContainerInstance(customKafkaListener);

			if (null != container) {

				container.setupMessageListener(consumerListenerFactory.getObject());
				container.setAutoStartup(customKafkaListener.getAutoStartup());
				container.setConcurrency(customKafkaListener.getConcurrency());
				container.setBeanName(id);

				ContainerProperties containerProperties = container.getContainerProperties();
				containerProperties.setClientId(customKafkaListener.getClientIdPrefix());
//				containerProperties.setAckMode(AckMode.MANUAL_IMMEDIATE);
				this.listenerContainers.put(id, container);
				startIfNecessary(container);
			}
		}
	}

	private ConcurrentMessageListenerContainer<String, String> createContainerInstance(
			CustomKafkaListener customKafkaListener) {
		TopicPartitionInitialOffset[] topicPartitions = resolveTopicPartitions(customKafkaListener);

		if (topicPartitions.length > 0) {
			ContainerProperties properties = new ContainerProperties(topicPartitions);
			return new ConcurrentMessageListenerContainer<String, String>(consumerFactory, properties);
		} else {
			String[] topics = customKafkaListener.getTopics();

			Assert.state(topics.length > 0, "At least one 'topics' required in CustomKafkaListener");
			if (topics.length > 0) {
				ContainerProperties properties = new ContainerProperties(topics);
				return new ConcurrentMessageListenerContainer<String, String>(consumerFactory, properties);
			} else {
				return null;
			}
		}
	}

	private void startIfNecessary(MessageListenerContainer listenerContainer) {
		if (listenerContainer.isAutoStartup()) {
			listenerContainer.start();
		}
	}

	private String getEndpointId(CustomKafkaListener customKafkaListener) {
		if (StringUtils.hasText(customKafkaListener.getId())) {
			return customKafkaListener.getId();
		} else {
			return GENERATED_ID_PREFIX + this.counter.getAndIncrement();
		}
	}

	private TopicPartitionInitialOffset[] resolveTopicPartitions(CustomKafkaListener customKafkaListener) {
		CustomKafkaListener.TopicPartition[] topicPartitions = customKafkaListener.getTopicPartitions();
		List<TopicPartitionInitialOffset> result = new ArrayList<>();
		if (topicPartitions.length > 0) {
			for (CustomKafkaListener.TopicPartition topicPartition : topicPartitions) {
				result.addAll(resolveTopicPartitionsList(topicPartition));
			}
		}
		return result.toArray(new TopicPartitionInitialOffset[result.size()]);
	}

	private List<TopicPartitionInitialOffset> resolveTopicPartitionsList(
			CustomKafkaListener.TopicPartition topicPartition) {
		String topic = topicPartition.getTopic();
		Assert.hasText(topic, "topic in @TopicPartition must not be empty");
		Integer[] partitions = topicPartition.getPartitions();
		CustomKafkaListener.TopicPartition.PartitionOffset[] partitionOffsets = topicPartition.getPartitionOffsets();
		Assert.state(partitions.length > 0 || partitionOffsets.length > 0,
				"At least one 'partition' or 'partitionOffset' required in @TopicPartition for topic '" + topic + "'");
		List<TopicPartitionInitialOffset> result = new ArrayList<>();
		for (int i = 0; i < partitions.length; i++) {
			result.add(new TopicPartitionInitialOffset(topic, partitions[i]));
		}

		for (CustomKafkaListener.TopicPartition.PartitionOffset partitionOffset : partitionOffsets) {
			TopicPartitionInitialOffset topicPartitionOffset = new TopicPartitionInitialOffset(topic,
					partitionOffset.getPartition(), partitionOffset.getInitialOffset(),
					partitionOffset.getRelativeToCurrent());
			if (!result.contains(topicPartitionOffset)) {
				result.add(topicPartitionOffset);
			} else {
				throw new IllegalArgumentException(
						String.format("@TopicPartition can't have the same partition configuration twice: [%s]",
								topicPartitionOffset));
			}
		}
		return result;
	}

	public MessageListenerContainer getListenerContainer(String containerId) {

		Assert.hasText(containerId, "Endpoint id must not be empty");
		return this.listenerContainers.get(containerId);
	}

	public Set<String> getListenerContainerIds() {

		return Collections.unmodifiableSet(this.listenerContainers.keySet());
	}

	public Set<String> getListenerContainerIdsWithStatus() {

		return Collections.unmodifiableSet(this.listenerContainers.entrySet().stream()
				.map(x -> x.getKey() + " is in " + (x.getValue().isRunning() ? "" : "not") + " running state")
				.collect(Collectors.toSet()));
	}

	public Collection<MessageListenerContainer> getListenerContainers() {

		return Collections.unmodifiableCollection(this.listenerContainers.values());
	}

	public void startListenerContainers() {

		for (MessageListenerContainer listenerContainer : this.getListenerContainers()) {
			if (!listenerContainer.isRunning()) {
				listenerContainer.start();
			}
		}
	}

	public void startListenerContainer(String containerId) {

		MessageListenerContainer listenerContainer = this.getListenerContainer(containerId);

		Assert.notNull(listenerContainer, "container does not exist");

		if (!listenerContainer.isRunning()) {
			listenerContainer.start();
		}
	}

	public void stopListenerContainers() {

		for (MessageListenerContainer listenerContainer : this.getListenerContainers()) {
			if (listenerContainer.isRunning()) {
				listenerContainer.stop();
			}
		}
	}

	public void stopListenerContainer(String containerId) {

		MessageListenerContainer listenerContainer = this.getListenerContainer(containerId);

		Assert.notNull(listenerContainer, "container does not exist");

		if (listenerContainer.isRunning()) {
			listenerContainer.stop();
		}
	}

	public void pauseListenerContainer(String containerId) {

		MessageListenerContainer listenerContainer = this.getListenerContainer(containerId);

		Assert.notNull(listenerContainer, "container does not exist");

		if (listenerContainer.isRunning()) {
			listenerContainer.pause();
		}
	}

	public void resumeListenerContainer(String containerId) {

		MessageListenerContainer listenerContainer = this.getListenerContainer(containerId);

		Assert.notNull(listenerContainer, "container does not exist");

		if (listenerContainer.isRunning()) {
			listenerContainer.resume();
		}
	}

	@Override
	public void stop(Runnable callback) {
		stopListenerContainers();
	}

	@PreDestroy
	public void destroy() {
		for (MessageListenerContainer listenerContainer : this.getListenerContainers()) {

			if (listenerContainer instanceof DisposableBean) {
				try {

					((DisposableBean) listenerContainer).destroy();
				} catch (Exception e) {
					logger.warn("Failed to destroy message listener container", e);
				}

			}
		}
	}

	@Override
	public void start() {
		/* do nothing for this */
	}

	@Override
	public void stop() {
		stopListenerContainers();
	}

	@Override
	public boolean isRunning() {
		for (MessageListenerContainer listenerContainer : this.getListenerContainers()) {
			if (listenerContainer.isRunning()) {
				return true;
			}
		}
		return false;
	}

}