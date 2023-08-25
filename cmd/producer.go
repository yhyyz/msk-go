package cmd

import (
	"context"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"
	"strconv"
	"strings"
	"time"
)

func BatchMsgCtx(ctx context.Context, values <-chan kafka.Message, maxItems int, maxTimeout time.Duration) chan []kafka.Message {
	batches := make(chan []kafka.Message)

	go func() {
		defer close(batches)

		for keepGoing := true; keepGoing; {
			var batch []kafka.Message
			expire := time.After(maxTimeout)

			for {
				select {
				case <-ctx.Done():
					keepGoing = false
					goto done

				case value, ok := <-values:
					if !ok {
						keepGoing = false
						goto done
					}

					batch = append(batch, value)
					if len(batch) == maxItems {
						goto done
					}
				case <-expire:
					goto done
				}
			}

		done:
			if len(batch) > 0 {
				batches <- batch
			}
		}
	}()
	return batches
}

func produce(cmd *cobra.Command) {
	broker, _ := cmd.Flags().GetString("broker")
	brokers := strings.Split(broker, ",")
	//brokers := strings.Split(viper.GetString("broker"), ",")
	topic, _ := cmd.Flags().GetString("topic")
	ack, _ := cmd.Flags().GetInt("ack")
	recordsNum, _ := cmd.Flags().GetInt("recordsNum")
	timeout, _ := cmd.Flags().GetInt("timeout")
	batchSize, _ := cmd.Flags().GetInt("batchSize")
	runningTime, _ := cmd.Flags().GetInt("runningTime")
	log.Logger.Info().Msgf("broker: %v topic: %v ack: %v recordsNum: %v batchTimeout: %v batchSize: %v, runningTime: %v", brokers, topic, ack, recordsNum, timeout, batchSize, runningTime)

	// kafka  Writer
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		Transport:    &kafka.Transport{},
		RequiredAcks: kafka.RequiredAcks(ack),
	}
	// write some data
	chmsg := make(chan kafka.Message, 10000)

	go func() {
		for i := 0; i < recordsNum; i++ {
			msg := kafka.Message{
				Key:   []byte("key" + strconv.Itoa(i)),
				Value: []byte("msg" + strconv.Itoa(i)),
			}
			chmsg <- msg
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(runningTime)*time.Millisecond)
	defer cancel()

	batches := BatchMsgCtx(ctx, chmsg, batchSize, time.Duration(timeout)*time.Millisecond)
	start := time.Now()
	var records = 0
	for batch := range batches {
		batchStart := time.Now()
		err := w.WriteMessages(context.Background(), batch...)
		if err != nil {
			log.Logger.Fatal().Msgf(err.Error())
		}
		records = records + len(batch)
		log.Logger.Info().Msgf("write batch data records %v, elapsed: %v", len(batch), time.Now().Sub(batchStart))
		log.Logger.Info().Msgf("total data records %v, elapsed: %v", records, time.Now().Sub(start))
	}

	if err := w.Close(); err != nil {
		log.Logger.Fatal().Msgf(err.Error())
	}
}

var producerCmd = &cobra.Command{
	Use:   "producer",
	Short: "msk kafka-go producer",
	Run: func(cmd *cobra.Command, args []string) {
		produce(cmd)
	},
}

func init() {
	producerCmd.Flags().String("broker", "localhost:9092", "msk broker")
	producerCmd.MarkFlagRequired("broker")
	producerCmd.Flags().String("topic", "", "producer topic")
	producerCmd.MarkFlagRequired("topic")
	producerCmd.Flags().Int("ack", -1, "ack")
	producerCmd.Flags().Int("recordsNum", 1000, "send records number")
	producerCmd.MarkFlagRequired("recordsNum")
	producerCmd.Flags().Int("batchSize", 100, "send records batch size")
	producerCmd.Flags().Int("timeout", 3000, "send batch records timeout")
	producerCmd.Flags().Int("runningTime", 600000, "total running time")

}
