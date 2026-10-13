package config

import "os"

type Config struct {
	HTTPAddr string
}

func Load() Config {
	HTTPAddr := os.Getenv("HTTP_ADDR")
	if HTTPAddr == "" {
		HTTPAddr = ":8080"
	}
	return Config{
		HTTPAddr: HTTPAddr,
	}
}
