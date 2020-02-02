package com.configurable.kafka.listner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties
@EnableConfigurationProperties
public class CustomKafkaListeners {
	
	List<CustomKafkaListener> kafkaListeners = new ArrayList<>();
	
	public List<CustomKafkaListener> getKafkaListeners() {
		return kafkaListeners;
	}

	public void setKafkaListeners(List<CustomKafkaListener> kafkaListeners) {
		this.kafkaListeners = kafkaListeners;
	}

	public static class CustomKafkaListener {

		/**
		 * The unique identifier of the container managing for this endpoint.
		 * <p[]If none is specified an auto-generated one is provided.
		 * <p[]Note: When provided, this value will override the group id property
		 * in the consumer factory configuration, unless {@link #idIsGroup()}
		 * is set to false.
		 * @return the {@code id} for the container managing for this endpoint.
		 * @see org.springframework.kafka.config.KafkaListenerEndpointRegistry#getListenerContainer(String)
		 */
		String id = "";

		/**
		 * The topics for this listener.
		 * The entries can be 'topic name', 'property-placeholder keys' or 'expressions'.
		 * Expression must be resolved to the topic name.
		 * Mutually exclusive with {@link #topicPattern()} and {@link #topicPartitions()}.
		 * @return the topic names or expressions (SpEL) to listen to.
		 */
		String[] topics = {};

		/**
		 * The topicPartitions for this listener.
		 * Mutually exclusive with {@link #topicPattern()} and {@link #topics()}.
		 * @return the topic names or expressions (SpEL) to listen to.
		 */
		TopicPartition[] topicPartitions = {};

		/**
		 * Override the container factory's {@code concurrency} setting for this listener. May
		 * be a property placeholder or SpEL expression that evaluates to a {@link Number}, in
		 * which case {@link Number#intValue()} is used to obtain the value.
		 * @return the concurrency.
		 * @since 2.2
		 */
		Integer concurrency = 1;
		
		/**
		 * When provided, overrides the client id property in the consumer factory
		 * configuration. A suffix ('-n') is added for each container instance to ensure
		 * uniqueness when concurrency is used.
		 * @return the client id prefix.
		 * @since 2.1.1
		 */
		String clientIdPrefix = "";

		/**
		 * Set to true or false, to override the default setting in the container factory. May
		 * be a property placeholder or SpEL expression that evaluates to a {@link Boolean} or
		 * a {@link String}, in which case the {@link Boolean#parseBoolean(String)} is used to
		 * obtain the value.
		 * @return true to auto start, false to not auto start.
		 * @since 2.2
		 */
		Boolean autoStartup = false;
		
		
		
		@Override
		public String toString() {
			return "CustomKafkaListener [id=" + id + ", topics=" + Arrays.toString(topics) + ", topicPartitions="
					+ Arrays.toString(topicPartitions) + ", concurrency=" + concurrency + ", clientIdPrefix="
					+ clientIdPrefix + ", autoStartup=" + autoStartup + "]";
		}

		public static class TopicPartition {

			/**
			 * The topic to listen on.
			 * @return the topic to listen on. Property place holders
			 * and SpEL expressions are supported, which must resolve
			 * to a String.
			 */
			String topic = "";

			/**
			 * The partitions within the topic.
			 * Partitions specified here can't be duplicated in {@link #partitionOffsets()}.
			 * @return the partitions within the topic. Property place
			 * holders and SpEL expressions are supported, which must
			 * resolve to Integers (or Strings that can be parsed as
			 * Integers).
			 */
			Integer[] partitions = {};

			/**
			 * The partitions with initial offsets within the topic.
			 * Partitions specified here can't be duplicated in the {@link #partitions()}.
			 * @return the {@link PartitionOffset} array.
			 */
			PartitionOffset[] partitionOffsets = {};
			
			
			
			@Override
			public String toString() {
				return "TopicPartition [topic=" + topic + ", partitions=" + Arrays.toString(partitions)
						+ ", partitionOffsets=" + Arrays.toString(partitionOffsets) + "]";
			}

			public static class PartitionOffset {

				/**
				 * The partition within the topic to listen on.
				 * Property place holders and SpEL expressions are supported,
				 * which must resolve to Integer (or String that can be parsed as Integer).
				 * @return partition within the topic.
				 */
				Integer partition;

				/**
				 * The initial offset of the {@link #partition()}.
				 * Property place holders and SpEL expressions are supported,
				 * which must resolve to Long (or String that can be parsed as Long).
				 * @return initial offset.
				 */
				Long initialOffset;

				/**
				 * By default, positive {@link #initialOffset()} is absolute, negative
				 * is relative to the current topic end. When this is 'true', the
				 * initial offset (positive or negative) is relative to the current
				 * consumer position.
				 * @return whether or not the offset is relative to the current position.
				 * @since 1.1
				 */
				Boolean relativeToCurrent = false;
				
				

				@Override
				public String toString() {
					return "PartitionOffset [partition=" + partition + ", initialOffset=" + initialOffset
							+ ", relativeToCurrent=" + relativeToCurrent + "]";
				}

				public Integer getPartition() {
					return partition;
				}

				public void setPartition(Integer partition) {
					this.partition = partition;
				}

				public Long getInitialOffset() {
					return initialOffset;
				}

				public void setInitialOffset(Long initialOffset) {
					this.initialOffset = initialOffset;
				}

				public Boolean getRelativeToCurrent() {
					return relativeToCurrent;
				}

				public void setRelativeToCurrent(Boolean relativeToCurrent) {
					this.relativeToCurrent = relativeToCurrent;
				}

			}

			public String getTopic() {
				return topic;
			}

			public void setTopic(String topic) {
				this.topic = topic;
			}

			public Integer[] getPartitions() {
				return partitions;
			}

			public void setPartitions(Integer[] partitions) {
				this.partitions = partitions;
			}

			public PartitionOffset[] getPartitionOffsets() {
				return partitionOffsets;
			}

			public void setPartitionOffsets(PartitionOffset[] partitionOffsets) {
				this.partitionOffsets = partitionOffsets;
			}


		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String[] getTopics() {
			return topics;
		}

		public void setTopics(String[] topics) {
			this.topics = topics;
		}

		public TopicPartition[] getTopicPartitions() {
			return topicPartitions;
		}

		public void setTopicPartitions(TopicPartition[] topicPartitions) {
			this.topicPartitions = topicPartitions;
		}

		public String getClientIdPrefix() {
			return clientIdPrefix;
		}

		public void setClientIdPrefix(String clientIdPrefix) {
			this.clientIdPrefix = clientIdPrefix;
		}

		public Integer getConcurrency() {
			return concurrency;
		}

		public void setConcurrency(Integer concurrency) {
			this.concurrency = concurrency;
		}

		public Boolean getAutoStartup() {
			return autoStartup;
		}

		public void setAutoStartup(Boolean autoStartup) {
			this.autoStartup = autoStartup;
		}


	}
}
