package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

// rootCmd 代表没有调用子命令时的基础命令
var rootCmd = &cobra.Command{
	Use:   "msk",
	Short: "msk cmd",
	// Run: func(cmd *cobra.Command, args []string) { },
}

func Execute() {
	rootCmd.AddCommand(consumerCmd)
	rootCmd.AddCommand(producerCmd)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
