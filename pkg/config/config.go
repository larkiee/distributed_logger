package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log"
	"os"
	"strings"

	"github.com/spf13/viper"
)

var (
	caFile, serverCertFile, serverKeyFile, clientCertFile, clientKeyFile string
)

type TLSRequest struct {
	IsServer   bool
	ServerAddr string
}

func getFilePath(name string) string {
	crendsPath := viper.GetString("tls.crendsPath")
	log.Println("crendsPath: ", crendsPath)
	crendsPath = strings.TrimSuffix(crendsPath, "/")

	return crendsPath + "/" + name
}

func GetTLSConfig(r TLSRequest) (*tls.Config, error) {
	tlsConfig := &tls.Config{}
	var cert tls.Certificate
	var err error
	if r.IsServer {
		cert, err = tls.LoadX509KeyPair(
			serverCertFile,
			serverKeyFile,
		)
	} else {
		cert, err = tls.LoadX509KeyPair(
			clientCertFile,
			clientKeyFile,
		)
	}

	if err != nil {
		return nil, err
	}
	certs := []tls.Certificate{cert}
	if r.IsServer {
		tlsConfig.Certificates = certs
	}

	b, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	ca := x509.NewCertPool()
	OK := ca.AppendCertsFromPEM(b)
	if !OK {
		return nil, errors.New("error in appending ca")
	}
	if r.IsServer {
		tlsConfig.ClientCAs = ca
		tlsConfig.ClientAuth = tls.VerifyClientCertIfGiven
	} else {
		tlsConfig.RootCAs = ca
	}
	tlsConfig.ServerName = r.ServerAddr
	return tlsConfig, nil
}