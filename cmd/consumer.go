package cmd

import (
	"context"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"
	"strings"
	"time"
)

func closeReader(r *kafka.Reader) {
	if err := r.Close(); err != nil {
		log.Logger.Err(err).Msg("Error closing the reader")
	}
}

func consume(cmd *cobra.Command) {
	broker, _ := cmd.Flags().GetString("broker")
	brokers := strings.Split(broker, ",")
	//brokers := strings.Split(viper.GetString("broker"), ",")
	topic, _ := cmd.Flags().GetString("topic")
	consumerGroup, _ := cmd.Flags().GetString("group")
	offset, _ := cmd.Flags().GetString("offset")
	commitSize, _ := cmd.Flags().GetInt("commitSize")
	startOffset := kafka.LastOffset
	if offset == "earliest" {
		startOffset = kafka.FirstOffset
	}

	log.Logger.Info().Msgf("broker: %q topic: %q group: %q", brokers, topic, consumerGroup)
	// readConfig
	kafkaReadConfig := kafka.ReaderConfig{
		Brokers:     *&brokers,
		GroupID:     consumerGroup,
		GroupTopics: []string{topic},
		// earliest
		StartOffset: startOffset,
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		},
	}

	// read data
	r := kafka.NewReader(kafkaReadConfig)
	defer closeReader(r)

	//for {
	//	m, err := r.ReadMessage(context.Background())
	//	if err != nil {
	//		log.Logger.Error().Msgf("read message error %q", err.Error())
	//		panic(err)
	//	}
	//	log.Logger.Info().Msgf("msk-data %s", m.Value)
	//}

	ctx := context.Background()
	commitNum := 0
	batchNum := 0
	var msgArr []kafka.Message
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			log.Logger.Error().Msgf("fetch messages error:", err)
			break
		}
		commitNum = commitNum + 1

		msgArr = append(msgArr, m)
		log.Logger.Info().Msgf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		if commitNum >= commitSize {
			batchNum = batchNum + 1
			if err := r.CommitMessages(ctx, msgArr...); err != nil {
				log.Logger.Error().Msgf("failed to commit messages:", err)
			}
			log.Logger.Info().Msgf("commit msg records batch %v, records %v", batchNum, len(msgArr))
			commitNum = 0
			msgArr = msgArr[0:0]
		}
	}
}

var consumerCmd = &cobra.Command{
	Use:   "consumer",
	Short: "msk kafka-go consumer",
	Run: func(cmd *cobra.Command, args []string) {
		consume(cmd)
	},
}

func init() {
	consumerCmd.Flags().String("broker", "localhost:9092", "msk broker")
	consumerCmd.MarkFlagRequired("broker")
	consumerCmd.Flags().String("topic", "", "consumer topic")
	consumerCmd.MarkFlagRequired("topic")
	consumerCmd.Flags().String("group", "", "consumer group")
	consumerCmd.MarkFlagRequired("group")
	consumerCmd.Flags().String("offset", "", "consumer offset, earliest/latest")
	consumerCmd.Flags().Int("commitSize", 100, "the number of records commit for each offset")
}
