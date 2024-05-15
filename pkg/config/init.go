package config

import (
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/viper"
)

func init() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("/home/amir/projects/distributed_logger/pkg/server")
	viper.ReadInConfig()
	// default server settings
	viper.SetDefault("server.port", 0)
	viper.SetDefault("server.ip", "127.0.0.1")
	// default tls settings
	viper.SetDefault("tls.use", true)
	viper.SetDefault("tls.crendsPath", "./config/tls/crends")
	log.Println("tls.use", viper.GetBool("tls.use"))
	log.Println("tls.crendsPath", viper.GetString("tls.crendsPath"))

	caFile = getFilePath("ca.pem")
	serverCertFile = getFilePath("server.pem")
	serverKeyFile = getFilePath("server-key.pem")
	clientCertFile = getFilePath("client.pem")
	clientKeyFile = getFilePath("client-key.pem")

	cd, _ := os.Getwd()

	if viper.GetBool("tls.use") && !strings.Contains(cd, "server") && !strings.Contains(cd, "agent") {
		crendsPath := viper.GetString("tls.crendsPath")
		makeCmd := exec.Command("make", "gencert")

		if err := makeCmd.Run(); err != nil {
			log.Fatal("error in generating credentials")
		}

		exec.Command("mkdir", "-p", crendsPath).Run()
		moveCmd := exec.Command(
			"mv",
			"server.pem", "server.csr", "server-key.pem",
			"ca.pem", "ca.csr", "ca-key.pem",
			"client.pem", "client.csr", "client-key.pem",
			crendsPath)

		if err := moveCmd.Run(); err != nil {
			log.Fatal("error in generating credentials 2")
		}
	}
}
